use std::sync::Arc;

use anyhow::Result;
use futures::FutureExt;
use mylibsql::{init, rusqlite, Log, Primary};
use parking_lot::Mutex;
use uuid::Uuid;

#[tokio::test]
async fn micro_replication_test() -> Result<()> {
    let (blank_db, log) = init(|conn| Ok(conn.execute_batch("create table stuff(uuid)")?)).await?;

    let mut primary = Primary::open(&blank_db, &[]).await?;
    let mut frames = primary.capture_frames();
    let logs_store = Arc::new(Mutex::new(vec![]));
    let replicated_log = Arc::new(Mutex::new(Log::create(log.next_frame_no()).await?));

    fn insert_rows(conn: &rusqlite::Connection, count: usize) -> Result<()> {
        let mut stmt = conn.prepare("insert into stuff(uuid) values (?)")?;
        for _ in 0..count {
            stmt.execute([Uuid::new_v4().to_string()])?;
        }
        Ok(())
    }

    fn count_rows(conn: &mut rusqlite::Connection) -> Result<usize> {
        Ok(conn.query_row("select count(*) from stuff", [], |row| row.get(0))?)
    }

    let ack = primary
        .with_connection(|conn| insert_rows(conn, 10))
        .await?;

    assert_eq!(10, primary.with_connection(count_rows).await?.await);

    {
        let replicated_log = replicated_log.clone();
        while let Ok(next_frames) = frames.try_next().await {
            replicated_log.lock().push_frames(next_frames).await?;
        }
        let last_replicated_frame = replicated_log.lock().last_frame_no().unwrap();
        frames.ack_replicated(last_replicated_frame);
    }

    // checkpoint
    {
        let logs_store = logs_store.clone();
        let frame_no = primary
            .checkpoint(move |log| async move { logs_store.lock().push(log) }.boxed())
            .await?
            .await;
        assert_eq!(Some(11), frame_no);
        replicated_log.lock().truncate(12).await?;
    };

    assert_eq!(10, primary.with_connection(count_rows).await?.await);
    assert!(ack.now_or_never().is_some());

    // empty checkpoint
    {
        let logs_store = logs_store.clone();
        let frame_no = primary
            .checkpoint(move |log| {
                async move {
                    assert!(log.is_empty());
                    logs_store.lock().push(log)
                }
                .boxed()
            })
            .await?
            .await;
        assert_eq!(None, frame_no);
    };

    assert_eq!(10, primary.with_connection(count_rows).await?.await);

    let ack1 = primary.with_connection(|conn| insert_rows(conn, 4)).await?;

    {
        let replicated_log = replicated_log.clone();
        while let Ok(next_frames) = frames.try_next().await {
            replicated_log.lock().push_frames(next_frames).await?;
        }
        let last_replicated_frame = replicated_log.lock().last_frame_no().unwrap();
        dbg!(last_replicated_frame);
        frames.ack_replicated(last_replicated_frame);
    }

    let mut ack2 = primary.with_connection(|conn| insert_rows(conn, 7)).await?;
    assert!(ack1.now_or_never().is_some());
    assert!((&mut ack2).now_or_never().is_none());

    {
        let replicated_log = replicated_log.clone();
        while let Ok(next_frames) = frames.try_next().await {
            replicated_log.lock().push_frames(next_frames).await?;
        }
        let last_replicated_frame = replicated_log.lock().last_frame_no().unwrap();
        dbg!(last_replicated_frame);
        frames.ack_replicated(last_replicated_frame);
    }

    // checkpoint
    {
        let logs_store = logs_store.clone();
        let frame_no = primary
            .checkpoint(move |log| async move { logs_store.lock().push(log) }.boxed())
            .await?
            .await;
        assert_eq!(Some(22), frame_no);
        replicated_log.lock().truncate(23).await?;
    };

    assert_eq!(21, primary.with_connection(count_rows).await?.await);

    let mut acks = vec![];
    for _ in 0..6 {
        acks.push(primary.with_connection(|conn| insert_rows(conn, 1)).await?);
        {
            let replicated_log = replicated_log.clone();
            while let Ok(next_frames) = frames.try_next().await {
                replicated_log.lock().push_frames(next_frames).await?;
            }
            let last_replicated_frame = replicated_log.lock().last_frame_no().unwrap();
            dbg!(last_replicated_frame);
            frames.ack_replicated(last_replicated_frame);
        }
    }

    for _ in 0..3 {
        acks.push(primary.with_connection(|conn| insert_rows(conn, 1)).await?);
    }

    assert_eq!(30, primary.with_connection(count_rows).await?.await);

    // Primary crash
    drop(primary);

    // 3 inserts have been lost
    assert_eq!(
        6,
        acks.into_iter()
            .filter_map(|ack| ack.now_or_never())
            .count()
    );

    assert_eq!(replicated_log.lock().start_frame_no(), 23);
    assert_eq!(replicated_log.lock().last_frame_no(), Some(28));
    assert_eq!(logs_store.lock().len(), 3);

    // Restore

    // inject missing log
    let mut logs = logs_store.lock().clone();
    logs.push(replicated_log.lock().clone());

    let primary = Primary::open(&blank_db, &logs).await?;

    // yes 3 inserts have been lost
    assert_eq!(27, primary.with_connection(count_rows).await?.await);

    Ok(())
}
