use libsql::Database;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let db_file = tempfile::NamedTempFile::new().unwrap();
    println!("Database {}", db_file.path().display());

    let auth_token = std::env::var("TURSO_AUTH_TOKEN").expect("Expected a TURSO_AUTH_TOKEN");

    let opts = libsql::Opts::with_http_sync("http://localhost:8080".to_owned(), auth_token);
    let db = Database::open_with_opts(db_file.path().to_str().unwrap(), opts)
        .await
        .unwrap();
    let conn = db.connect().unwrap();

    loop {
        match db.sync().await {
            Ok(frames_applied) => {
                if frames_applied == 0 {
                    println!("No more frames at the moment! See you later");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
                println!("Applied {frames_applied} frames");
            }
            Err(e) => {
                println!("Error: {e}");
                break;
            }
        }
        let response = conn.query("SELECT * FROM sqlite_master", ()).unwrap();
        let rows = match response {
            Some(rows) => rows,
            None => {
                println!("No rows");
                continue;
            }
        };
        while let Ok(Some(row)) = rows.next() {
            println!(
                "| {:024} | {:024} | {:024} | {:024} |",
                row.get::<&str>(0).unwrap(),
                row.get::<&str>(1).unwrap(),
                row.get::<&str>(2).unwrap(),
                row.get::<&str>(3).unwrap(),
            );
        }
    }
}
