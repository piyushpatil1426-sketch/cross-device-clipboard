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
            created_at  TEXT NOT NULL,
            ocr_text    TEXT
        )",
        [],
    ).expect("Failed to create table");

    fs::create_dir_all("clip_images").expect("Failed to create clip_images folder");

    let mut clipboard = Clipboard::new().expect("Failed to access clipboard");
    let mut last_text = String::new();
    let mut last_image_size: usize = 0;

    println!("Clipboard watcher started. Writing to clipboard.db ...");

    loop {
        let mut handled_this_tick = false;

        // --- Text ---
        if let Ok(current_text) = clipboard.get_text() {
            if current_text != last_text && !current_text.is_empty() {
                save_clip_with_ocr(&conn, &current_text, "text", None);
                last_text = current_text.clone();
                handled_this_tick = true;
            }
        }

        // --- Image ---
        if !handled_this_tick {
            if let Ok(img_data) = clipboard.get_image() {
                let byte_len = img_data.bytes.len();

                if byte_len != last_image_size && byte_len > 0 {
                    let width = img_data.width as u32;
                    let height = img_data.height as u32;
                    let buffer = RgbaImage::from_raw(width, height, img_data.bytes.into_owned());

                    if let Some(img) = buffer {
                        let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
                        let filename = format!("clip_images/{}.png", timestamp);

                        img.save_with_format(&filename, ImageFormat::Png)
                            .expect("Failed to save image");

                        let ocr_text = run_ocr(&filename);

                        save_clip_with_ocr(&conn, &filename, "image", ocr_text.as_deref());
                        last_image_size = byte_len;

                        println!("Saved screenshot: {}", filename);
                        if let Some(text) = &ocr_text {
                            println!("OCR extracted {} chars", text.len());
                        }
                    }
                }
            }
        }

        thread::sleep(Duration::from_millis(500));
    }
}

// Runs Tesseract OCR on an image file and returns the extracted text, if any.
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

// Inserts a clip into the database. ocr_text is None for text clips,
// and Some(...) or None for image clips depending on whether OCR found anything.
fn save_clip_with_ocr(conn: &Connection, content: &str, content_type: &str, ocr_text: Option<&str>) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    conn.execute(
        "INSERT INTO clips (content, content_type, device_id, created_at, ocr_text)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![content, content_type, "windows-pc", timestamp, ocr_text],
    ).expect("Failed to insert clip");

    println!("Saved {} clip at {}", content_type, timestamp);
}