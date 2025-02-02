use anyhow::Result;
use futures::FutureExt;
use mylibsql::Snapshot;

/// cargo run --bin generate_test_data
#[tokio::main]
async fn main() -> Result<()> {
    let (snapshot, log) = mylibsql::init(|conn| {
        let sql = std::fs::read_to_string("tests/data/acme/0.sql")?;
        conn.execute_batch(&sql)?;
        Ok(())
    })
    .await?;

    snapshot.move_to("tests/data/acme/0.db").await?;
    log.copy_to("tests/data/acme/0.log").await?;

    let snapshot = Snapshot::open("tests/data/acme/0.db").await?;
    let primary = mylibsql::Primary::open(&snapshot, &[]).await?;

    for epoch in 1..=3 {
        let sql = std::fs::read_to_string(format!("tests/data/acme/{epoch}.sql"))?;
        let _ = primary
            .with_connection(move |conn| Ok(conn.execute_batch(&sql)?))
            .await?;
        primary
            .checkpoint(move |log| {
                async move {
                    log.copy_to(format!("tests/data/acme/{epoch}.log"))
                        .await
                        .unwrap();
                }
                .boxed()
            })
            .await?
            .await;
    }

    Ok(())
}
