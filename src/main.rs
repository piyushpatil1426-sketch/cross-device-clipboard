use arboard::Clipboard;
use rusqlite::Connection;
use std::thread;
use std::time::Duration;
use std::fs;
use chrono::Local;
use image::{RgbaImage, ImageFormat};
use sha2::{Sha256, Digest};

fn main() {
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

    fs::create_dir_all("clip_images").expect("Failed to create clip_images folder");

    let mut clipboard = Clipboard::new().expect("Failed to access clipboard");

    // Instead of separately tracking last_text and last_image_size, we track
    // ONE hash of "whatever the clipboard held last time we checked" — since
    // the clipboard can only hold one thing at a time anyway, this is simpler
    // and more correct than two separate variables.
    let mut last_hash = String::new();

    println!("Clipboard watcher started. Writing to clipboard.db ...");

    loop {
        let mut handled_this_tick = false;

        // --- Text ---
        if let Ok(current_text) = clipboard.get_text() {
            if !current_text.is_empty() {
                let hash = hash_bytes(current_text.as_bytes());

                if hash != last_hash {
                    save_clip_with_ocr(&conn, &current_text, "text", None, &hash);
                    last_hash = hash;
                    handled_this_tick = true;
                }
            }
        }

        // --- Image ---
        if !handled_this_tick {
            if let Ok(img_data) = clipboard.get_image() {
                if !img_data.bytes.is_empty() {
                    // Hash the raw pixel bytes BEFORE we do anything else with them.
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

                            save_clip_with_ocr(&conn, &filename, "image", ocr_text.as_deref(), &hash);
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

// Takes any bytes (text as bytes, or raw image pixels) and returns a
// consistent, fixed-length hex string fingerprint of them.
fn hash_bytes(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    // `{:x}` formats bytes as lowercase hex. `result` is a fixed-size byte
    // array; this turns it into a readable string like "a3f9c1...".
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