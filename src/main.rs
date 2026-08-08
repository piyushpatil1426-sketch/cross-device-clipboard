use arboard::Clipboard;
use rusqlite::Connection;
use std::thread;
use std::time::Duration;
use std::fs;
use chrono::Local;
use image::{RgbaImage, ImageFormat};

fn main() {
    let conn = Connection::open("clipboard.db").expect("Failed to open database");

    conn.execute(
        "CREATE TABLE IF NOT EXISTS clips (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            content     TEXT NOT NULL,
            content_type TEXT NOT NULL,
            device_id   TEXT NOT NULL,
            created_at  TEXT NOT NULL
        )",
        [],
    ).expect("Failed to create table");

    // Make a folder to hold saved screenshot images, so the DB just stores a path,
    // not the raw image bytes (keeps the DB small and fast).
    fs::create_dir_all("clip_images").expect("Failed to create clip_images folder");

    let mut clipboard = Clipboard::new().expect("Failed to access clipboard");
    let mut last_text = String::new();
    // We track the last image by its byte length as a cheap "did it change" check.
    // (Not perfect, but good enough for now — a proper hash comes later.)
    let mut last_image_size: usize = 0;

    println!("Clipboard watcher started. Writing to clipboard.db ...");

    loop {
        // --- Try text first ---
        let mut handled_this_tick = false;

        if let Ok(current_text) = clipboard.get_text() {
            if current_text != last_text && !current_text.is_empty() {
                save_clip(&conn, &current_text, "text");
                last_text = current_text.clone();
                handled_this_tick = true;
            }
        }

        // --- If it wasn't new text, check if it's a new image ---
        if !handled_this_tick {
            if let Ok(img_data) = clipboard.get_image() {
                let byte_len = img_data.bytes.len();

                if byte_len != last_image_size && byte_len > 0 {
                    // arboard gives us raw RGBA pixels + width/height.
                    // We rebuild that into an RgbaImage the `image` crate understands.
                    let width = img_data.width as u32;
                    let height = img_data.height as u32;
                    let buffer = RgbaImage::from_raw(width, height, img_data.bytes.into_owned());

                    if let Some(img) = buffer {
                        let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
                        let filename = format!("clip_images/{}.png", timestamp);

                        img.save_with_format(&filename, ImageFormat::Png)
                            .expect("Failed to save image");

                        save_clip(&conn, &filename, "image");
                        last_image_size = byte_len;

                        println!("Saved screenshot: {}", filename);
                    }
                }
            }
        }

        thread::sleep(Duration::from_millis(500));
    }
}

// A small helper function so we don't repeat the INSERT logic for text vs images.
// `content` is either the clipped text itself, or a file path (for images).
fn save_clip(conn: &Connection, content: &str, content_type: &str) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    conn.execute(
        "INSERT INTO clips (content, content_type, device_id, created_at)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![content, content_type, "windows-pc", timestamp],
    ).expect("Failed to insert clip");

    println!("Saved {} clip at {}", content_type, timestamp);
}