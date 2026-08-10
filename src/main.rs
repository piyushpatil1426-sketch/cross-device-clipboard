use arboard::Clipboard;
use rusqlite::Connection;
use std::thread;
use std::time::Duration;
use std::fs;
use std::env;
use chrono::Local;
use image::{RgbaImage, ImageFormat};
use sha2::{Sha256, Digest};
use tiny_http::{Server, Response, Header};
use serde::Serialize;

// This struct represents one clip as we'll send it to the browser as JSON.
// `#[derive(Serialize)]` auto-generates the code to convert this into JSON —
// without it, we'd have to hand-write that conversion ourselves.
#[derive(Serialize)]
struct ClipJson {
    id: i64,
    content: String,
    content_type: String,
    created_at: String,
    ocr_text: Option<String>,
}

fn main() {
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

    if args.len() >= 3 && args[1] == "search" {
        let query = &args[2];
        run_search(&conn, query);
    } else if args.len() >= 2 && args[1] == "serve" {
        run_server(conn);
    } else {
        run_watcher(&conn);
    }
}

// --- WEB SERVER ---

fn run_server(conn: Connection) {
    let server = Server::http("127.0.0.1:8080").expect("Failed to start server");
    println!("Server running at http://127.0.0.1:8080");

    // `server.incoming_requests()` blocks and waits for each incoming HTTP request,
    // one at a time — this is a simple single-threaded server, which is fine for
    // a local single-user tool like this.
    for request in server.incoming_requests() {
        let url = request.url().to_string();

        if url == "/" || url == "/index.html" {
            serve_html(request);
        } else if url.starts_with("/api/clips") {
            serve_clips_json(&conn, request, None);
        } else if url.starts_with("/api/search") {
            // Extract the "q" parameter manually from something like "/api/search?q=hello"
            let query_param = url
                .split('?')
                .nth(1)
                .and_then(|qs| qs.split('&').find(|p| p.starts_with("q=")))
                .map(|p| p.trim_start_matches("q="))
                .map(|q| urlencoding_decode(q));

            serve_clips_json(&conn, request, query_param);
        } else if url.starts_with("/clip_images/") {
            serve_image_file(request, &url);
        } else {
            let response = Response::from_string("Not found").with_status_code(404);
            let _ = request.respond(response);
        }
    }
}

// A very small, dependency-free URL-decoder for the common cases (spaces as %20 or +).
// Good enough for our search box; not a full RFC-compliant decoder.
fn urlencoding_decode(s: &str) -> String {
    s.replace('+', " ").replace("%20", " ")
}

fn serve_html(request: tiny_http::Request) {
    // include_str! embeds the file's contents into the compiled binary at
    // COMPILE time (not read from disk at runtime). The path is relative to
    // this source file's location.
    let html = include_str!("../static/index.html");
    let header = Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap();
    let response = Response::from_string(html).with_header(header);
    let _ = request.respond(response);
}

fn serve_clips_json(conn: &Connection, request: tiny_http::Request, search_query: Option<String>) {
    let clips = match &search_query {
        Some(q) if !q.trim().is_empty() => fetch_clips(conn, Some(q)),
        _ => fetch_clips(conn, None),
    };

    let json = serde_json::to_string(&clips).expect("Failed to serialize clips");
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let response = Response::from_string(json).with_header(header);
    let _ = request.respond(response);
}

fn serve_image_file(request: tiny_http::Request, url: &str) {
    // url looks like "/clip_images/20260810_075408.png" — strip the leading slash
    // to turn it into a relative file path we can open.
    let path = url.trim_start_matches('/');

    match fs::read(path) {
        Ok(bytes) => {
            let header = Header::from_bytes(&b"Content-Type"[..], &b"image/png"[..]).unwrap();
            let response = Response::from_data(bytes).with_header(header);
            let _ = request.respond(response);
        }
        Err(_) => {
            let response = Response::from_string("Image not found").with_status_code(404);
            let _ = request.respond(response);
        }
    }
}

// Shared by both the plain "list everything" case and the "search" case.
fn fetch_clips(conn: &Connection, search_query: Option<&str>) -> Vec<ClipJson> {
    let (sql, pattern);
    if let Some(q) = search_query {
        sql = "SELECT id, content, content_type, created_at, ocr_text
               FROM clips
               WHERE content LIKE ?1 OR ocr_text LIKE ?1
               ORDER BY created_at DESC LIMIT 100";
        pattern = format!("%{}%", q);
    } else {
        sql = "SELECT id, content, content_type, created_at, ocr_text
               FROM clips
               ORDER BY created_at DESC LIMIT 100";
        pattern = String::new(); // unused in this branch, but needed to satisfy the type
    }

    let mut stmt = conn.prepare(sql).expect("Failed to prepare query");

    let rows_iter = if search_query.is_some() {
        stmt.query_map(rusqlite::params![pattern], row_to_clip)
    } else {
        stmt.query_map([], row_to_clip)
    }.expect("Failed to run query");

    rows_iter.filter_map(|r| r.ok()).collect()
}

fn row_to_clip(row: &rusqlite::Row) -> rusqlite::Result<ClipJson> {
    Ok(ClipJson {
        id: row.get(0)?,
        content: row.get(1)?,
        content_type: row.get(2)?,
        created_at: row.get(3)?,
        ocr_text: row.get(4)?,
    })
}

// --- CLI SEARCH (unchanged from before) ---

fn run_search(conn: &Connection, query: &str) {
    let pattern = format!("%{}%", query);

    let mut stmt = conn.prepare(
        "SELECT id, content, content_type, created_at, ocr_text
         FROM clips
         WHERE content LIKE ?1 OR ocr_text LIKE ?1
         ORDER BY created_at DESC"
    ).expect("Failed to prepare search query");

    let results = stmt.query_map(rusqlite::params![pattern], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
        ))
    }).expect("Failed to run search query");

    println!("Search results for \"{}\":\n", query);

    let mut count = 0;
    for row in results {
        let (id, content, content_type, created_at, ocr_text) = row.expect("Row error");
        count += 1;

        println!("[{}] ({}) {}", id, content_type, created_at);
        if content_type == "text" {
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

// --- WATCHER (unchanged from before) ---

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