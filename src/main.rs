use arboard::Clipboard;
use rusqlite::Connection;
use std::thread;
use std::time::Duration;
use std::fs;
use std::env;
use chrono::Local;
use image::{RgbaImage, ImageFormat};
use sha2::{Sha256, Digest};

fn main() {
    // env::args() gives us the command-line arguments. The first one (index 0)
    // is always the program's own path, so real arguments start at index 1.
    // .collect() turns the iterator into a Vec<String> we can inspect normally.
    let args: Vec<String> = env::args().collect();

    let conn = Connection::open("clipboard.db").expect("Failed to open database");

    conn.execute(
        "CREATE TABLE IF NOT EXISTS clips (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            content     TEXT NOT NULL,
            content_type TEXT NOT NULL,
            device_id   TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            ocr_text    TEXT,
            content_hash TEXT NOT NULL
        )",
        [],
    ).expect("Failed to create table");

    // If the user ran: cargo run -- search "some query"
    // args[1] will be "search" and args[2] will be the query text.
    if args.len() >= 3 && args[1] == "search" {
        let query = &args[2];
        run_search(&conn, query);
    } else {
        run_watcher(&conn);
    }
}

// Searches both `content` and `ocr_text` columns for the query, case-insensitively.
fn run_search(conn: &Connection, query: &str) {
    // SQL's LIKE is case-insensitive by default for ASCII in SQLite.
    // We wrap the query in % wildcards to match it anywhere in the text.
    let pattern = format!("%{}%", query);

    // prepare() compiles the SQL once; we then feed it parameters and iterate rows.
    let mut stmt = conn.prepare(
        "SELECT id, content, content_type, created_at, ocr_text
         FROM clips
         WHERE content LIKE ?1 OR ocr_text LIKE ?1
         ORDER BY created_at DESC"
    ).expect("Failed to prepare search query");

    // query_map runs the query and lets us transform each row into a Rust value.
    // Here we build a tuple of the columns we care about.
    let results = stmt.query_map(rusqlite::params![pattern], |row| {
        Ok((
            row.get::<_, i64>(0)?,           // id
            row.get::<_, String>(1)?,        // content
            row.get::<_, String>(2)?,        // content_type
            row.get::<_, String>(3)?,        // created_at
            row.get::<_, Option<String>>(4)?, // ocr_text (might be NULL)
        ))
    }).expect("Failed to run search query");

    println!("Search results for \"{}\":\n", query);

    let mut count = 0;
    for row in results {
        let (id, content, content_type, created_at, ocr_text) = row.expect("Row error");
        count += 1;

        println!("[{}] ({}) {}", id, content_type, created_at);
        if content_type == "text" {
            // Show a short preview instead of dumping potentially huge text.
            let preview: String = content.chars().take(100).collect();
            println!("  {}", preview);
        } else {
            println!("  file: {}", content);
            if let Some(text) = ocr_text {
                let preview: String = text.chars().take(100).collect();
                println!("  ocr:  {}", preview);
            }
        }
        println!();
    }

    if count == 0 {
        println!("No matches found.");
    } else {
        println!("{} match(es) found.", count);
    }
}

// This is your existing watcher loop, unchanged, just moved into its own function.
fn run_watcher(conn: &Connection) {
    fs::create_dir_all("clip_images").expect("Failed to create clip_images folder");

    let mut clipboard = Clipboard::new().expect("Failed to access clipboard");
    let mut last_hash = String::new();

    println!("Clipboard watcher started. Writing to clipboard.db ...");

    loop {
        let mut handled_this_tick = false;

        if let Ok(current_text) = clipboard.get_text() {
            if !current_text.is_empty() {
                let hash = hash_bytes(current_text.as_bytes());

                if hash != last_hash {
                    save_clip_with_ocr(conn, &current_text, "text", None, &hash);
                    last_hash = hash;
                    handled_this_tick = true;
                }
            }
        }

        if !handled_this_tick {
            if let Ok(img_data) = clipboard.get_image() {
                if !img_data.bytes.is_empty() {
                    let hash = hash_bytes(&img_data.bytes);

                    if hash != last_hash {
                        let width = img_data.width as u32;
                        let height = img_data.height as u32;
                        let buffer = RgbaImage::from_raw(width, height, img_data.bytes.into_owned());

                        if let Some(img) = buffer {
                            let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
                            let filename = format!("clip_images/{}.png", timestamp);

                            img.save_with_format(&filename, ImageFormat::Png)
                                .expect("Failed to save image");

                            let ocr_text = run_ocr(&filename);

                            save_clip_with_ocr(conn, &filename, "image", ocr_text.as_deref(), &hash);
                            last_hash = hash;

                            println!("Saved screenshot: {}", filename);
                            if let Some(text) = &ocr_text {
                                println!("OCR extracted {} chars", text.len());
                            }
                        }
                    }
                }
            }
        }

        thread::sleep(Duration::from_millis(500));
    }
}

fn hash_bytes(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    format!("{:x}", result)
}

fn run_ocr(image_path: &str) -> Option<String> {
    use rusty_tesseract::{Image, Args};

    let img = Image::from_path(image_path).ok()?;
    let args = Args::default();

    match rusty_tesseract::image_to_string(&img, &args) {
        Ok(text) => {
            let trimmed = text.trim().to_string();
            if trimmed.is_empty() { None } else { Some(trimmed) }
        }
        Err(_) => None,
    }
}

fn save_clip_with_ocr(
    conn: &Connection,
    content: &str,
    content_type: &str,
    ocr_text: Option<&str>,
    content_hash: &str,
) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    conn.execute(
        "INSERT INTO clips (content, content_type, device_id, created_at, ocr_text, content_hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![content, content_type, "windows-pc", timestamp, ocr_text, content_hash],
    ).expect("Failed to insert clip");

    println!("Saved {} clip at {}", content_type, timestamp);
}