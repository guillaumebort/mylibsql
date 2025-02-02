use anyhow::Result;
use futures::FutureExt;
use mylibsql::{self, Ack, Log, Primary, Snapshot};

#[tokio::test]
async fn basic_usage_with_acme_data() -> Result<()> {
    let primary = Primary::open(
        &Snapshot::open("tests/data/acme/0.db").await?,
        &[
            Log::open("tests/data/acme/1.log").await?,
            Log::open("tests/data/acme/2.log").await?,
            Log::open("tests/data/acme/3.log").await?,
        ],
    )
    .await?;

    // sanity check
    let x: Ack<usize> = primary
        .with_connection(|conn| {
            Ok(conn.query_row("select count(*) from company", (), |row| row.get(0))?)
        })
        .await?;
    assert_eq!(x.now_or_never(), Some(2));
    let x: Ack<String> = primary
        .with_connection(|conn| {
            Ok(conn.query_row(
                "select name from company order by salary desc ",
                (),
                |row| row.get(0),
            )?)
        })
        .await?;
    assert_eq!(x.now_or_never(), Some("David".to_string()));

    // primary looks ready, we can use it
    let mut ack = primary
        .with_connection(|conn| {
            Ok(conn.execute(
                "insert into company values (7, 'Bob', 36, 'bam', 95700.00 )",
                (),
            )?)
        })
        .await?;

    // ack is pending
    assert!((&mut ack).now_or_never().is_none());

    // but we can read our own writes
    let x: Ack<String> = primary
        .with_connection(|conn| {
            Ok(conn.query_row(
                "select name from company order by salary desc ",
                (),
                |row| row.get(0),
            )?)
        })
        .await?;
    assert_eq!(x.now_or_never(), Some("Bob".to_string()));

    // let's checkpoint
    primary
        .checkpoint(|log| {
            async move {
                assert_eq!(log.last_frame_no(), Some(19));
            }
            .boxed()
        })
        .await?
        .await;

    // so now previous ack is ready
    assert_eq!(ack.now_or_never(), Some(1));

    // what if we checkpoint again?
    primary
        .checkpoint(|log| {
            async move {
                assert_eq!(log.last_frame_no(), Some(19));
            }
            .boxed()
        })
        .await?
        .await;

    // delete a row
    let mut ack = primary
        .with_connection(|conn| Ok(conn.execute("delete from company where name = 'David'", ())?))
        .await?;

    // ack is pending
    assert!((&mut ack).now_or_never().is_none());

    // so we can't move to replica
    let Err((_, primary)) = primary.into_replica() else {
        panic!("primary should not be able to move to replica with pending acks");
    };

    // and checkpoint
    primary
        .checkpoint(|log| {
            async move {
                assert_eq!(log.last_frame_no(), Some(21));
            }
            .boxed()
        })
        .await?
        .await;

    // so now we can move to replica
    let replica = primary.into_replica().unwrap();

    let names = replica
        .with_connection(|conn| {
            let mut stmt = conn.prepare("select name, salary from company order by salary asc ")?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            let mut names: Vec<(String, f64)> = vec![];
            for row in rows {
                names.push(row?);
            }
            Ok(names)
        })
        .await?;
    assert_eq!(
        names,
        vec![("Mark".to_string(), 70000.0), ("Bob".to_string(), 95700.0)]
    );

    Ok(())
}
