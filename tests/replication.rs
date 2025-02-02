use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use futures::FutureExt;
use mylibsql::{init, Primary, Replica};
use parking_lot::Mutex;
use rand::Rng;
use uuid::Uuid;

#[tokio::test]
async fn replication_test() -> Result<()> {
    replication(Duration::from_secs(1), Duration::from_millis(100)).await
}

#[tokio::test]
#[ignore = "slow test, run manually"]
async fn replication_test_slow_1() -> Result<()> {
    replication(Duration::from_secs(30), Duration::from_millis(100)).await
}

#[tokio::test]
#[ignore = "slow test, run manually"]
async fn replication_test_slow_2() -> Result<()> {
    replication(Duration::from_secs(30), Duration::ZERO).await
}

#[tokio::test]
#[ignore = "slow test, run manually"]
async fn replication_test_slow_3() -> Result<()> {
    replication(Duration::from_secs(30), Duration::from_secs(1)).await
}

#[tokio::test]
#[ignore = "slow test, run manually"]
async fn replication_forever() -> Result<()> {
    replication(Duration::MAX, Duration::ZERO).await
}

// 1 task inject rows into the primary database (with some random transaction rollbacks) and checkpoints every 100ms
// 1 task reads the logs and replays them into the replica
// at the end of the test, the replica should see the same number of rows as the primary
async fn replication(test_duration: Duration, max_replication_latency: Duration) -> Result<()> {
    let (blank_db, _) = init(|conn| Ok(conn.execute_batch("create table stuff(uuid)")?)).await?;

    let primary = Primary::open(blank_db.reopen().await?, vec![]).await?;
    let mut replica = Replica::open(blank_db.reopen().await?, vec![]).await?;

    let logs_store = Arc::new(Mutex::new(VecDeque::new()));
    let acks = Arc::new(Mutex::new(Vec::new()));

    println!("running for {}s... ", test_duration.as_secs());
    println!("primary: {primary:?}");
    println!("replica: {replica:?}");

    let write_loop = tokio::spawn({
        let logs_store = logs_store.clone();
        let acks = acks.clone();
        async move {
            let mut inserted = 0;
            let start = Instant::now();
            let mut t = Instant::now();
            let mut epoch = 0;
            loop {
                if let Ok(ack) = primary
                    .with_connection(|conn| {
                        let txn = conn.transaction()?;
                        let count = rand::thread_rng().gen_range(1..=1_000);
                        for _ in 0..count {
                            txn.execute(
                                "insert into stuff VALUES (?1)",
                                [Uuid::new_v4().to_string()],
                            )?;
                        }
                        // randomly rollback the transaction
                        if rand::thread_rng().gen_bool(0.75) {
                            txn.commit()?;
                            Ok(count)
                        } else {
                            txn.rollback()?;
                            Err(anyhow!("oops"))
                        }
                    })
                    .await
                {
                    inserted += ack.peek();
                    acks.lock().push(ack);
                }

                // checkpoint every 100ms
                if t.elapsed() > Duration::from_millis(100) {
                    epoch += 1;
                    let epoch = epoch;
                    println!("primary inserted {inserted} batches at epoch {epoch}");
                    let logs_store = logs_store.clone();
                    // checkpoint and fork to store the log
                    tokio::spawn(
                        primary
                            .checkpoint(move |log| {
                                // random delay
                                let push_latency =
                                    Duration::from_millis(rand::thread_rng().gen_range(
                                        0..=max_replication_latency.as_millis().try_into().unwrap(),
                                    ));
                                async move {
                                    tokio::time::sleep(push_latency).await;
                                    logs_store.lock().push_back(Some((epoch, log, inserted)));
                                }
                                .boxed()
                            })
                            .await?,
                    );
                    t = Instant::now();
                }

                // stop after test_duration
                if start.elapsed() > test_duration {
                    primary
                        .checkpoint(move |log| {
                            epoch += 1;
                            async move {
                                println!("primary inserted {inserted} batches at epoch {epoch}");
                                logs_store.lock().push_back(Some((epoch, log, inserted)));
                                logs_store.lock().push_back(None);
                            }
                            .boxed()
                        })
                        .await?
                        .await;
                    return anyhow::Ok(inserted);
                }
            }
        }
    });

    let replication_loop = tokio::spawn({
        let logs_store = logs_store.clone();
        async move {
            loop {
                async fn count_rows(replica: &Replica) -> Result<i32> {
                    replica
                        .with_connection(|conn| {
                            Ok(conn.query_row("select count(*) from stuff", (), |row| row.get(0))?)
                        })
                        .await
                }

                loop {
                    let log = logs_store.lock().pop_front();
                    match log {
                        Some(Some((epoch, log, inserted))) => {
                            replica.replicate(log).await?;
                            let count = count_rows(&replica).await?;
                            println!("replica sees {} batches at epoch {epoch}", count);
                            if count != inserted {
                                panic!(
                                    "!!!! replica sees {} batches, expected {}",
                                    count, inserted
                                );
                            }
                        }
                        Some(None) => {
                            return anyhow::Ok(count_rows(&replica).await?);
                        }
                        None => {
                            break;
                        }
                    }
                }

                // random delay
                let read_latency = Duration::from_millis(
                    rand::thread_rng()
                        .gen_range(0..=max_replication_latency.as_millis().try_into().unwrap()),
                );
                tokio::time::sleep(read_latency).await;
            }
        }
    });

    let total_seen_by_replica = replication_loop.await??;
    let total_inserted = write_loop.await??;

    println!("TOTAL inserted: {}", total_inserted);
    println!("TOTAL seen by replica: {}", total_seen_by_replica);

    assert_eq!(total_inserted, total_seen_by_replica);

    // also check all the acks (at this point all writes have been replicated and acked)
    let mut total_seen_by_acks = 0;
    for ack in Arc::into_inner(acks)
        .expect("wat? who is still owning acks?")
        .into_inner()
    {
        total_seen_by_acks += ack.await;
    }
    println!("TOTAL seen by acks: {}", total_seen_by_acks);
    assert_eq!(total_inserted, total_seen_by_acks);

    Ok(())
}
