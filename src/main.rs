use arboard::Clipboard;
use rusqlite::Connection;
use std::thread;
use std::time::Duration;
use std::fs;
use std::env;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use chrono::Local;
use image::{RgbaImage, ImageFormat};
use sha2::{Sha256, Digest};
use tiny_http::{Server, Response, Header};
use serde::Serialize;

#[derive(Serialize)]
struct ClipJson {
    id: i64,
    content: String,
    content_type: String,
    created_at: String,
    ocr_text: Option<String>,
    language: Option<String>,
    pinned: bool,
    is_sensitive: bool,
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() >= 3 && args[1] == "search" {
        let conn = open_db();
        run_search(&conn, &args[2]);
    } else if args.len() >= 2 && args[1] == "watch" {
        let conn = open_db();
        let paused = Arc::new(AtomicBool::new(false));
        run_watcher(&conn, paused);
    } else if args.len() >= 2 && args[1] == "serve" {
        let conn = open_db();
        let paused = Arc::new(AtomicBool::new(false));
        run_server(conn, paused);
    } else {
        run_combined();
    }
}

fn open_db() -> Connection {
    let conn = match Connection::open("clipboard.db") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: could not open clipboard.db ({e})");
            eprintln!("Check that the folder is writable and no other program has it locked.");
            process::exit(1);
        }
    };

    let create_result = conn.execute(
        "CREATE TABLE IF NOT EXISTS clips (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            content     TEXT NOT NULL,
            content_type TEXT NOT NULL,
            device_id   TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            ocr_text    TEXT,
            content_hash TEXT NOT NULL,
            language    TEXT,
            pinned      INTEGER NOT NULL DEFAULT 0,
            is_sensitive INTEGER NOT NULL DEFAULT 0
        )",
        [],
    );

    if let Err(e) = create_result {
        eprintln!("Error: could not set up the database schema ({e})");
        process::exit(1);
    }

    conn
}

fn run_combined() {
    println!("Starting clipboard capture + web UI together...");

    let paused = Arc::new(AtomicBool::new(false));
    let watcher_paused = Arc::clone(&paused);

    thread::spawn(|| {
        run_hotkey_listener();
    });

    thread::spawn(move || {
        let conn = open_db();
        run_watcher(&conn, watcher_paused);
    });

    let conn = open_db();
    run_server(conn, paused);
}

// --- GLOBAL HOTKEY ---

fn run_hotkey_listener() {
    use device_query::{DeviceQuery, DeviceState, Keycode};

    let device_state = DeviceState::new();
    let mut was_pressed = false;

    println!("Hotkey listener started. Press Ctrl+Alt+H to open the clipboard UI.");

    loop {
        let keys: Vec<Keycode> = device_state.get_keys();
        let ctrl = keys.contains(&Keycode::LControl) || keys.contains(&Keycode::RControl);
        let alt = keys.contains(&Keycode::LAlt) || keys.contains(&Keycode::RAlt);
        let h_pressed = keys.contains(&Keycode::H);

        let combo_pressed = ctrl && alt && h_pressed;

        if combo_pressed && !was_pressed {
            open_browser_to_ui();
        }

        was_pressed = combo_pressed;

        thread::sleep(Duration::from_millis(100));
    }
}

fn open_browser_to_ui() {
    println!("Hotkey pressed — opening clipboard UI...");
    let result = process::Command::new("cmd")
        .args(["/C", "start", "http://127.0.0.1:8080"])
        .spawn();

    if let Err(e) = result {
        eprintln!("Warning: failed to open browser ({e})");
    }
}

// --- LANGUAGE DETECTION ---

fn detect_language(content: &str) -> String {
    let rules: &[(&str, &[&str])] = &[
        ("python", &["def ", "import ", "elif ", "self.", "print(", "    return"]),
        ("rust", &["fn ", "let mut", "impl ", "use std::", "->", "println!"]),
        ("javascript", &["function ", "const ", "=>", "console.log", "let ", "require("]),
        ("typescript", &["interface ", ": string", ": number", "export type"]),
        ("java", &["public class", "public static void main", "System.out.println"]),
        ("c", &["#include <stdio.h>", "int main(", "printf("]),
        ("cpp", &["#include <iostream>", "std::", "cout <<"]),
        ("csharp", &["using System;", "Console.WriteLine", "public class"]),
        ("php", &["<?php", "$this->", "echo "]),
        ("sql", &["SELECT ", "FROM ", "WHERE ", "INSERT INTO"]),
        ("html", &["<!DOCTYPE", "<div", "<html"]),
        ("css", &["{", "px;", "color:"]),
        ("shell", &["#!/bin/bash", "#!/bin/sh", "echo $"]),
        ("json", &["{\"", "\":", "null,"]),
    ];

    let mut best_language = "text";
    let mut best_score = 0;

    for (language, patterns) in rules {
        let score = patterns.iter().filter(|p| content.contains(**p)).count();
        if score > best_score {
            best_score = score;
            best_language = language;
        }
    }

    if best_score >= 2 {
        best_language.to_string()
    } else {
        "text".to_string()
    }
}

// --- SENSITIVE DATA DETECTION ---

// Flags likely secrets (passwords, API keys, tokens, private keys) using
// keyword/pattern matching — same lightweight heuristic style as language
// detection. This only sets a warning badge; the clip is still captured
// and shown normally, per the chosen scope.
fn detect_sensitive(content: &str) -> bool {
    let lower = content.to_lowercase();

    let keywords = [
        "password", "passwd", "pwd=", "pwd:",
        "secret", "api_key", "apikey", "api-key",
        "access_token", "auth_token", "client_secret",
        "private_key", "-----begin", "bearer ",
        "aws_secret", "akia",
    ];

    keywords.iter().any(|k| lower.contains(k))
}

// --- WEB SERVER ---

fn run_server(conn: Connection, paused: Arc<AtomicBool>) {
    let server = match Server::http("127.0.0.1:8080") {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: could not start the web server ({e})");
            eprintln!("Is port 8080 already in use? Maybe another instance is already running.");
            process::exit(1);
        }
    };

    println!("Server running at http://127.0.0.1:8080");

    for request in server.incoming_requests() {
        let url = request.url().to_string();
        let method = request.method().clone();

        if url == "/" || url == "/index.html" {
            serve_html(request);
        } else if url == "/api/status" {
            serve_status(&paused, request);
        } else if url == "/api/pause" && method == tiny_http::Method::Post {
            paused.store(true, Ordering::Relaxed);
            println!("Capture paused.");
            serve_status(&paused, request);
        } else if url == "/api/resume" && method == tiny_http::Method::Post {
            paused.store(false, Ordering::Relaxed);
            println!("Capture resumed.");
            serve_status(&paused, request);
        } else if url.starts_with("/api/clips/") && url.ends_with("/toggle-pin") && method == tiny_http::Method::Post {
            toggle_pin(&conn, request, &url);
        } else if url.starts_with("/api/clips/") && method == tiny_http::Method::Delete {
            delete_clip(&conn, request, &url);
        } else if url.starts_with("/api/clips") {
            serve_clips_json(&conn, request, None);
        } else if url.starts_with("/api/search") {
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

fn serve_status(paused: &Arc<AtomicBool>, request: tiny_http::Request) {
    let is_paused = paused.load(Ordering::Relaxed);
    let json = format!("{{\"paused\":{}}}", is_paused);
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let response = Response::from_string(json).with_header(header);
    let _ = request.respond(response);
}

fn urlencoding_decode(s: &str) -> String {
    s.replace('+', " ").replace("%20", " ")
}

fn serve_html(request: tiny_http::Request) {
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

    let json = match serde_json::to_string(&clips) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("Warning: failed to serialize clips to JSON ({e})");
            let response = Response::from_string("[]").with_status_code(500);
            let _ = request.respond(response);
            return;
        }
    };

    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let response = Response::from_string(json).with_header(header);
    let _ = request.respond(response);
}

fn serve_image_file(request: tiny_http::Request, url: &str) {
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

fn delete_clip(conn: &Connection, request: tiny_http::Request, url: &str) {
    let id_str = url.trim_start_matches("/api/clips/");
    let id: i64 = match id_str.parse() {
        Ok(n) => n,
        Err(_) => {
            let response = Response::from_string("Invalid clip id").with_status_code(400);
            let _ = request.respond(response);
            return;
        }
    };

    let row: Result<(String, String), _> = conn.query_row(
        "SELECT content, content_type FROM clips WHERE id = ?1",
        rusqlite::params![id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    );

    let (content, content_type) = match row {
        Ok(data) => data,
        Err(_) => {
            let response = Response::from_string("Clip not found").with_status_code(404);
            let _ = request.respond(response);
            return;
        }
    };

    let delete_result = conn.execute("DELETE FROM clips WHERE id = ?1", rusqlite::params![id]);

    match delete_result {
        Ok(_) => {
            if content_type == "image" {
                if let Err(e) = fs::remove_file(&content) {
                    eprintln!("Warning: could not delete image file {} ({e})", content);
                }
            }
            println!("Deleted clip {}", id);
            let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
            let response = Response::from_string("{\"status\":\"deleted\"}").with_header(header);
            let _ = request.respond(response);
        }
        Err(e) => {
            eprintln!("Error: failed to delete clip {} ({e})", id);
            let response = Response::from_string("Failed to delete").with_status_code(500);
            let _ = request.respond(response);
        }
    }
}

// Handles POST /api/clips/{id}/toggle-pin — flips the pinned flag and
// returns the new state.
fn toggle_pin(conn: &Connection, request: tiny_http::Request, url: &str) {
    let trimmed = url.trim_start_matches("/api/clips/").trim_end_matches("/toggle-pin");
    let id: i64 = match trimmed.parse() {
        Ok(n) => n,
        Err(_) => {
            let response = Response::from_string("Invalid clip id").with_status_code(400);
            let _ = request.respond(response);
            return;
        }
    };

    let current: Result<bool, _> = conn.query_row(
        "SELECT pinned FROM clips WHERE id = ?1",
        rusqlite::params![id],
        |r| r.get(0),
    );

    let current_pinned = match current {
        Ok(v) => v,
        Err(_) => {
            let response = Response::from_string("Clip not found").with_status_code(404);
            let _ = request.respond(response);
            return;
        }
    };

    let new_pinned = !current_pinned;
    let update_result = conn.execute(
        "UPDATE clips SET pinned = ?1 WHERE id = ?2",
        rusqlite::params![new_pinned, id],
    );

    match update_result {
        Ok(_) => {
            let json = format!("{{\"pinned\":{}}}", new_pinned);
            let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
            let response = Response::from_string(json).with_header(header);
            let _ = request.respond(response);
        }
        Err(e) => {
            eprintln!("Error: failed to toggle pin for clip {} ({e})", id);
            let response = Response::from_string("Failed to update").with_status_code(500);
            let _ = request.respond(response);
        }
    }
}

fn fetch_clips(conn: &Connection, search_query: Option<&str>) -> Vec<ClipJson> {
    let (sql, pattern);
    if let Some(q) = search_query {
        sql = "SELECT id, content, content_type, created_at, ocr_text, language, pinned, is_sensitive
               FROM clips
               WHERE content LIKE ?1 OR ocr_text LIKE ?1
               ORDER BY pinned DESC, created_at DESC LIMIT 100";
        pattern = format!("%{}%", q);
    } else {
        sql = "SELECT id, content, content_type, created_at, ocr_text, language, pinned, is_sensitive
               FROM clips
               ORDER BY pinned DESC, created_at DESC LIMIT 100";
        pattern = String::new();
    }

    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Warning: failed to prepare query ({e})");
            return Vec::new();
        }
    };

    let rows_iter = if search_query.is_some() {
        stmt.query_map(rusqlite::params![pattern], row_to_clip)
    } else {
        stmt.query_map([], row_to_clip)
    };

    match rows_iter {
        Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
        Err(e) => {
            eprintln!("Warning: query failed ({e})");
            Vec::new()
        }
    }
}

fn row_to_clip(row: &rusqlite::Row) -> rusqlite::Result<ClipJson> {
    Ok(ClipJson {
        id: row.get(0)?,
        content: row.get(1)?,
        content_type: row.get(2)?,
        created_at: row.get(3)?,
        ocr_text: row.get(4)?,
        language: row.get(5)?,
        pinned: row.get(6)?,
        is_sensitive: row.get(7)?,
    })
}

// --- CLI SEARCH ---

fn run_search(conn: &Connection, query: &str) {
    let pattern = format!("%{}%", query);

    let mut stmt = match conn.prepare(
        "SELECT id, content, content_type, created_at, ocr_text, language, pinned, is_sensitive
         FROM clips
         WHERE content LIKE ?1 OR ocr_text LIKE ?1
         ORDER BY pinned DESC, created_at DESC"
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: search query failed to prepare ({e})");
            return;
        }
    };

    let results = match stmt.query_map(rusqlite::params![pattern], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, bool>(6)?,
            row.get::<_, bool>(7)?,
        ))
    }) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: search failed to run ({e})");
            return;
        }
    };

    println!("Search results for \"{}\":\n", query);

    let mut count = 0;
    for row in results {
        let (id, content, content_type, created_at, ocr_text, language, pinned, is_sensitive) = match row {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Warning: skipping a row that failed to read ({e})");
                continue;
            }
        };
        count += 1;

        let lang_label = language.unwrap_or_else(|| "text".to_string());
        let flags = format!(
            "{}{}",
            if pinned { " [pinned]" } else { "" },
            if is_sensitive { " [sensitive]" } else { "" }
        );
        println!("[{}] ({} / {}){} {}", id, content_type, lang_label, flags, created_at);
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

// --- WATCHER ---

fn run_watcher(conn: &Connection, paused: Arc<AtomicBool>) {
    if let Err(e) = fs::create_dir_all("clip_images") {
        eprintln!("Error: could not create clip_images folder ({e})");
        return;
    }

    let mut clipboard = None;
    for attempt in 1..=3 {
        match Clipboard::new() {
            Ok(cb) => {
                clipboard = Some(cb);
                break;
            }
            Err(e) => {
                eprintln!("Warning: could not access clipboard (attempt {attempt}/3): {e}");
                thread::sleep(Duration::from_millis(500));
            }
        }
    }

    let mut clipboard = match clipboard {
        Some(cb) => cb,
        None => {
            eprintln!("Error: could not access the clipboard after 3 attempts. Watcher stopping.");
            return;
        }
    };

    let mut last_hash = String::new();

    println!("Clipboard watcher started. Writing to clipboard.db ...");

    loop {
        if paused.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(500));
            continue;
        }

        let mut handled_this_tick = false;

        if let Ok(current_text) = clipboard.get_text() {
            if !current_text.is_empty() {
                let hash = hash_bytes(current_text.as_bytes());

                if hash != last_hash {
                    let language = detect_language(&current_text);
                    let is_sensitive = detect_sensitive(&current_text);
                    if save_clip_with_ocr(conn, &current_text, "text", None, &hash, &language, is_sensitive) {
                        last_hash = hash;
                        handled_this_tick = true;
                    }
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

                            match img.save_with_format(&filename, ImageFormat::Png) {
                                Ok(()) => {
                                    let ocr_text = run_ocr(&filename);
                                    let is_sensitive = ocr_text
                                        .as_deref()
                                        .map(detect_sensitive)
                                        .unwrap_or(false);

                                    if save_clip_with_ocr(conn, &filename, "image", ocr_text.as_deref(), &hash, "n/a", is_sensitive) {
                                        last_hash = hash;
                                        println!("Saved screenshot: {}", filename);
                                        if let Some(text) = &ocr_text {
                                            println!("OCR extracted {} chars", text.len());
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Warning: failed to save screenshot to {} ({e})", filename);
                                    last_hash = hash;
                                }
                            }
                        } else {
                            eprintln!("Warning: clipboard image data didn't match its reported width/height, skipping.");
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

    let img = match Image::from_path(image_path) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("Warning: OCR could not open image {} ({e})", image_path);
            return None;
        }
    };
    let args = Args::default();

    match rusty_tesseract::image_to_string(&img, &args) {
        Ok(text) => {
            let trimmed = text.trim().to_string();
            if trimmed.is_empty() { None } else { Some(trimmed) }
        }
        Err(e) => {
            eprintln!("Warning: OCR failed on {} ({e}) — is Tesseract installed and on PATH?", image_path);
            None
        }
    }
}

fn save_clip_with_ocr(
    conn: &Connection,
    content: &str,
    content_type: &str,
    ocr_text: Option<&str>,
    content_hash: &str,
    language: &str,
    is_sensitive: bool,
) -> bool {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    let result = conn.execute(
        "INSERT INTO clips (content, content_type, device_id, created_at, ocr_text, content_hash, language, is_sensitive)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![content, content_type, "windows-pc", timestamp, ocr_text, content_hash, language, is_sensitive],
    );

    match result {
        Ok(_) => {
            let flag = if is_sensitive { " [flagged as sensitive]" } else { "" };
            println!("Saved {} clip ({}){} at {}", content_type, language, flag, timestamp);
            true
        }
        Err(e) => {
            eprintln!("Error: failed to save clip to database ({e})");
            false
        }
    }
}