#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use arboard::Clipboard;
use rusqlite::Connection;
use std::thread;
use std::time::Duration;
use std::fs;
use std::env;
use std::process;
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::io::{self, Read, Write};
use std::collections::{HashMap, HashSet};
use chrono::Local;
use image::{RgbaImage, ImageFormat};
use sha2::{Sha256, Digest};
use tiny_http::{Server, Response, Header};
use serde::{Serialize, Deserialize};
use rand::Rng;
use uuid::Uuid;

// --- LOGGING ---

fn log_line(prefix: &str, msg: &str) {
    let _ = writeln!(io::stdout(), "{}", msg);

    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open("clipboard_daemon.log") {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let _ = writeln!(file, "[{}] {} {}", timestamp, prefix, msg);
    }
}

macro_rules! log_info {
    ($($arg:tt)*) => {
        log_line("INFO", &format!($($arg)*))
    };
}

macro_rules! log_warn {
    ($($arg:tt)*) => {
        log_line("WARN", &format!($($arg)*))
    };
}

const HTTP_TIMEOUT_SECS: u64 = 5;
const DISCOVERY_PORT: u16 = 45679;

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
    digest: Option<String>,
}

// --- DEVICE IDENTITY ---

#[derive(Serialize, Deserialize, Clone)]
struct DeviceConfig {
    device_id: String,
    device_name: String,
    secret: String,
}

fn load_or_create_device_config() -> DeviceConfig {
    let path = "device_config.json";

    if let Ok(contents) = fs::read_to_string(path) {
        if let Ok(config) = serde_json::from_str::<DeviceConfig>(&contents) {
            return config;
        }
    }

    let device_id = Uuid::new_v4().to_string();
    let device_name = whoami_hostname();
    let secret = generate_secret();

    let config = DeviceConfig { device_id, device_name, secret };

    let json = serde_json::to_string_pretty(&config).expect("Failed to serialize device config");
    if let Err(e) = fs::write(path, json) {
        log_warn!("Warning: could not save device_config.json ({e})");
    }

    config
}

fn whoami_hostname() -> String {
    env::var("COMPUTERNAME").unwrap_or_else(|_| "unknown-device".to_string())
}

fn generate_secret() -> String {
    let mut rng = rand::thread_rng();
    (0..32).map(|_| format!("{:x}", rng.gen_range(0..16))).collect()
}

// --- PEER MANAGEMENT ---

#[derive(Serialize, Deserialize, Clone)]
struct Peer {
    name: String,
    ip: String,
    port: u16,
    secret: String,
}

#[derive(Serialize, Deserialize, Default)]
struct PeerList {
    peers: Vec<Peer>,
}

fn load_peers() -> PeerList {
    match fs::read_to_string("peers.json") {
        Ok(contents) => serde_json::from_str(&contents).unwrap_or_default(),
        Err(_) => PeerList::default(),
    }
}

fn save_peers(peers: &PeerList) {
    let json = serde_json::to_string_pretty(peers).expect("Failed to serialize peers");
    if let Err(e) = fs::write("peers.json", json) {
        log_warn!("Warning: could not save peers.json ({e})");
    }
}

fn add_peer(name: &str, ip: &str, port: u16, secret: &str) {
    let mut peer_list = load_peers();
    peer_list.peers.retain(|p| p.name != name);

    peer_list.peers.push(Peer {
        name: name.to_string(),
        ip: ip.to_string(),
        port,
        secret: secret.to_string(),
    });

    save_peers(&peer_list);
    log_info!("Added peer '{}' at {}:{}", name, ip, port);
}

fn remove_peer(name: &str) {
    let mut peer_list = load_peers();
    let before = peer_list.peers.len();
    peer_list.peers.retain(|p| p.name != name);

    if peer_list.peers.len() < before {
        save_peers(&peer_list);
        log_info!("Removed peer '{}'", name);
    }
}

fn list_peers_cli() {
    let peer_list = load_peers();
    if peer_list.peers.is_empty() {
        println!("No peers configured yet. Use 'add-peer' to add one.");
        return;
    }
    println!("Configured peers:");
    for peer in &peer_list.peers {
        println!("  {} — {}:{}", peer.name, peer.ip, peer.port);
    }
}

// --- LAN DISCOVERY ---

#[derive(Clone, Serialize)]
struct DiscoveredDevice {
    device_id: String,
    device_name: String,
    ip: String,
    port: u16,
    last_seen: i64,
}

type DiscoveryMap = Arc<Mutex<HashMap<String, DiscoveredDevice>>>;

// Sends a short burst of "I'm here" broadcast packets (instead of a
// continuous loop) so other running instances can discover this device.
// Triggered on-demand: at startup, when the hotkey opens the UI, or when
// the user opens the pairing/sync panel — never on a timer.
fn announce_presence_once(device_id: String, device_name: String) {
    thread::spawn(move || {
        let socket = match UdpSocket::bind("0.0.0.0:0") {
            Ok(s) => s,
            Err(e) => {
                log_warn!("Discovery: failed to bind broadcast socket ({e})");
                return;
            }
        };

        if let Err(e) = socket.set_broadcast(true) {
            log_warn!("Discovery: failed to enable broadcast ({e})");
            return;
        }

        let message = serde_json::json!({
            "device_id": device_id,
            "device_name": device_name,
            "port": 8080u16,
        }).to_string();

        let target = format!("255.255.255.255:{}", DISCOVERY_PORT);

        // A handful of pulses (not an infinite loop) so peers whose UDP
        // packet gets dropped once still have a couple more chances.
        for _ in 0..3 {
            let _ = socket.send_to(message.as_bytes(), &target);
            thread::sleep(Duration::from_millis(300));
        }

        log_info!("Discovery: announced presence on the LAN.");
    });
}

// Listens for other devices' broadcast packets and records them in a shared
// map the UI can query. Ignores our own broadcasts.
fn run_discovery_listener(my_device_id: String, discovered: DiscoveryMap) {
    let socket = match UdpSocket::bind(format!("0.0.0.0:{}", DISCOVERY_PORT)) {
        Ok(s) => s,
        Err(e) => {
            log_warn!("Discovery: failed to bind listener ({e})");
            return;
        }
    };

    log_info!("Discovery listener started.");

    let mut buf = [0u8; 512];
    loop {
        match socket.recv_from(&mut buf) {
            Ok((len, src_addr)) => {
                if let Ok(text) = std::str::from_utf8(&buf[..len]) {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(text) {
                        let device_id = parsed.get("device_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        if device_id.is_empty() || device_id == my_device_id {
                            continue;
                        }

                        let device_name = parsed.get("device_name").and_then(|v| v.as_str()).unwrap_or("Unknown").to_string();
                        let port = parsed.get("port").and_then(|v| v.as_u64()).unwrap_or(8080) as u16;
                        let ip = src_addr.ip().to_string();
                        let now = Local::now().timestamp();

                        let mut map = discovered.lock().unwrap_or_else(|p| p.into_inner());
                        map.insert(device_id.clone(), DiscoveredDevice {
                            device_id, device_name, ip, port, last_seen: now,
                        });
                    }
                }
            }
            Err(e) => {
                log_warn!("Discovery: error receiving broadcast ({e})");
            }
        }
    }
}

// --- PAIRING (request / approve / auto-complete) ---

#[derive(Clone, Serialize)]
struct PendingRequest {
    device_id: String,
    device_name: String,
    ip: String,
    port: u16,
    secret: String,
}

type PendingRequestsMap = Arc<Mutex<HashMap<String, PendingRequest>>>;

// --- SYNC ---

#[derive(Serialize, Deserialize, Clone)]
struct SyncClip {
    content: String,
    content_type: String,
    device_id: String,
    created_at: String,
    ocr_text: Option<String>,
    content_hash: String,
    language: Option<String>,
    is_sensitive: bool,
    digest: Option<String>,
}

#[derive(Serialize, Clone, Default)]
struct PeerSyncStatus {
    last_synced_at: Option<String>,
    last_imported: usize,
    last_error: Option<String>,
}

type SyncStatusMap = Arc<Mutex<HashMap<String, PeerSyncStatus>>>;

fn update_sync_status(status_map: &SyncStatusMap, peer_name: &str, synced_at: Option<String>, imported: usize, error: Option<String>) {
    let mut map = status_map.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    map.insert(peer_name.to_string(), PeerSyncStatus {
        last_synced_at: synced_at,
        last_imported: imported,
        last_error: error,
    });
}

// NOTE: there is intentionally no timer-based fallback loop anymore.
// Sync now runs strictly on-demand, from one of:
//   1. The Ctrl+Alt+H hotkey (open_browser_to_ui)
//   2. The manual POST /api/peers/:name/sync-now endpoint
//   3. A completed pairing handshake (handle_pairing_respond / handle_pairing_complete)
//   4. An incoming /api/sync/notify ping from a peer that just synced
fn sync_all_peers(device_id: &str, status_map: &SyncStatusMap) {
    let peer_list = load_peers();
    if peer_list.peers.is_empty() {
        return;
    }
    let conn = open_db();
    for peer in &peer_list.peers {
        sync_with_peer(&conn, peer, device_id, status_map);
    }
}

// Tells every known peer "something changed on my end, pull it now" — used
// after a new clip is captured, and (via notify_peer_to_pull) for the
// bidirectional catch-up that runs at startup and right after pairing.
fn notify_all_peers_to_pull() {
    thread::spawn(|| {
        let peer_list = load_peers();
        for peer in &peer_list.peers {
            notify_peer_to_pull(peer);
        }
    });
}

fn notify_peer_to_pull(peer: &Peer) {
    let url = format!("http://{}:{}/api/sync/notify", peer.ip, peer.port);
    let _ = ureq::post(&url)
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .call();
}

// Pushes a deletion to every connected peer the instant it happens locally
// (not on a timer, not on the next pull) so both devices stay identical
// while the connection is active. Peers apply this by content_hash and do
// NOT re-propagate it, so this never loops back and forth.
fn propagate_delete_to_peers(content_hash: String) {
    thread::spawn(move || {
        let peer_list = load_peers();
        for peer in &peer_list.peers {
            let url = format!("http://{}:{}/api/sync/delete", peer.ip, peer.port);
            let body = serde_json::json!({
                "content_hash": content_hash,
                "secret": peer.secret,
            }).to_string();

            let result = ureq::post(&url)
                .set("Content-Type", "application/json")
                .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
                .send_string(&body);

            if let Err(e) = result {
                log_warn!("Sync: failed to push delete to '{}' ({e})", peer.name);
            }
        }
    });
}

// Pushes a pin/unpin the instant it happens locally, same reasoning as
// propagate_delete_to_peers above.
fn propagate_pin_to_peers(content_hash: String, pinned: bool) {
    thread::spawn(move || {
        let peer_list = load_peers();
        for peer in &peer_list.peers {
            let url = format!("http://{}:{}/api/sync/pin", peer.ip, peer.port);
            let body = serde_json::json!({
                "content_hash": content_hash,
                "pinned": pinned,
                "secret": peer.secret,
            }).to_string();

            let result = ureq::post(&url)
                .set("Content-Type", "application/json")
                .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
                .send_string(&body);

            if let Err(e) = result {
                log_warn!("Sync: failed to push pin state to '{}' ({e})", peer.name);
            }
        }
    });
}

fn sync_with_peer(conn: &Connection, peer: &Peer, device_id: &str, status_map: &SyncStatusMap) {
    let url = format!("http://{}:{}/api/sync/pull?secret={}", peer.ip, peer.port, peer.secret);

    let response = match ureq::get(&url).timeout(Duration::from_secs(HTTP_TIMEOUT_SECS)).call() {
        Ok(resp) => resp,
        Err(e) => {
            log_warn!("Sync: could not reach peer '{}' ({e})", peer.name);
            update_sync_status(status_map, &peer.name, None, 0, Some(e.to_string()));
            return;
        }
    };

    let body = match response.into_string() {
        Ok(b) => b,
        Err(e) => {
            log_warn!("Sync: failed to read response from '{}' ({e})", peer.name);
            update_sync_status(status_map, &peer.name, None, 0, Some(e.to_string()));
            return;
        }
    };

    let remote_clips: Vec<SyncClip> = match serde_json::from_str(&body) {
        Ok(c) => c,
        Err(e) => {
            log_warn!("Sync: failed to parse response from '{}' ({e})", peer.name);
            update_sync_status(status_map, &peer.name, None, 0, Some(e.to_string()));
            return;
        }
    };

    let mut imported = 0;

    for clip in remote_clips {
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM clips WHERE content_hash = ?1)",
            rusqlite::params![clip.content_hash],
            |r| r.get(0),
        ).unwrap_or(false);

        if exists {
            break;
        }

        let local_content = if clip.content_type == "image" {
            match fetch_and_save_remote_image(&peer.ip, peer.port, &clip.content, &peer.name) {
                Some(path) => path,
                None => {
                    log_warn!("Sync: failed to fetch image {} from '{}'", clip.content, peer.name);
                    continue;
                }
            }
        } else {
            clip.content.clone()
        };

        let insert_result = conn.execute(
            "INSERT INTO clips (content, content_type, device_id, created_at, ocr_text, content_hash, language, is_sensitive, digest, pinned)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0)",
            rusqlite::params![local_content, clip.content_type, clip.device_id, clip.created_at, clip.ocr_text, clip.content_hash, clip.language, clip.is_sensitive, clip.digest],
        );

        if insert_result.is_ok() {
            imported += 1;
        }
    }

    if imported > 0 {
        log_info!("Sync: imported {} new clip(s) from '{}'", imported, peer.name);
    }

    let now = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    update_sync_status(status_map, &peer.name, Some(now), imported, None);
    let _ = device_id;
}

fn fetch_and_save_remote_image(ip: &str, port: u16, remote_path: &str, peer_name: &str) -> Option<String> {
    let url = format!("http://{}:{}/{}", ip, port, remote_path);
    let response = ureq::get(&url).timeout(Duration::from_secs(HTTP_TIMEOUT_SECS)).call().ok()?;

    let mut bytes: Vec<u8> = Vec::new();
    response.into_reader().read_to_end(&mut bytes).ok()?;

    let filename_only = remote_path.rsplit('/').next().unwrap_or(remote_path);
    let safe_peer_name: String = peer_name.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect();
    let local_path = format!("clip_images/sync_{}_{}", safe_peer_name, filename_only);

    fs::create_dir_all("clip_images").ok()?;
    fs::write(&local_path, bytes).ok()?;

    Some(local_path)
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() >= 3 && args[1] == "search" {
        let conn = open_db();
        run_search(&conn, &args[2]);
    } else if args.len() >= 2 && args[1] == "watch" {
        let conn = open_db();
        let paused = Arc::new(AtomicBool::new(false));
        let device_config = load_or_create_device_config();
        run_watcher(&conn, paused, device_config.device_id);
    } else if args.len() >= 2 && args[1] == "serve" {
        let conn = open_db();
        let paused = Arc::new(AtomicBool::new(false));
        let device_config = load_or_create_device_config();
        let status_map: SyncStatusMap = Arc::new(Mutex::new(HashMap::new()));
        let discovered: DiscoveryMap = Arc::new(Mutex::new(HashMap::new()));
        let pending: PendingRequestsMap = Arc::new(Mutex::new(HashMap::new()));
        run_server(conn, paused, device_config.secret, device_config.device_id, device_config.device_name, status_map, discovered, pending);
    } else if args.len() >= 2 && args[1] == "whoami" {
        let config = load_or_create_device_config();
        println!("Device name: {}", config.device_name);
        println!("Device ID:   {}", config.device_id);
        println!("Secret:      {}", config.secret);
        println!();
        println!("Share your device name, your LAN IP, port 8080, and this secret");
        println!("with any device you want to authorize to sync with you.");
    } else if args.len() >= 6 && args[1] == "add-peer" {
        let name = &args[2];
        let ip = &args[3];
        let port: u16 = args[4].parse().unwrap_or(8080);
        let secret = &args[5];
        add_peer(name, ip, port, secret);
    } else if args.len() >= 3 && args[1] == "remove-peer" {
        remove_peer(&args[2]);
    } else if args.len() >= 2 && args[1] == "list-peers" {
        list_peers_cli();
    } else {
        run_combined();
    }
}

fn open_db() -> Connection {
    let conn = match Connection::open("clipboard.db") {
        Ok(c) => c,
        Err(e) => {
            log_warn!("Error: could not open clipboard.db ({e})");
            log_warn!("Check that the folder is writable and no other program has it locked.");
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
            is_sensitive INTEGER NOT NULL DEFAULT 0,
            digest      TEXT
        )",
        [],
    );

    if let Err(e) = create_result {
        log_warn!("Error: could not set up the database schema ({e})");
        process::exit(1);
    }

    conn
}

fn run_combined() {
    log_info!("Starting clipboard capture + web UI together...");

    let device_config = load_or_create_device_config();
    log_info!("Device identity: {} ({})", device_config.device_name, device_config.device_id);

    let paused = Arc::new(AtomicBool::new(false));
    let watcher_paused = Arc::clone(&paused);
    let watcher_device_id = device_config.device_id.clone();

    let status_map: SyncStatusMap = Arc::new(Mutex::new(HashMap::new()));
    let sync_status_map = Arc::clone(&status_map);
    let sync_device_id = device_config.device_id.clone();

    let hotkey_device_id = device_config.device_id.clone();
    let hotkey_status_map = Arc::clone(&status_map);

    let discovered: DiscoveryMap = Arc::new(Mutex::new(HashMap::new()));
    let pending: PendingRequestsMap = Arc::new(Mutex::new(HashMap::new()));

    // The discovery listener stays running: it just blocks on a UDP socket
    // read (no polling, no wakeups, effectively zero CPU/battery cost while
    // idle) and is what lets this device be found when a peer announces.
    let listener_device_id = device_config.device_id.clone();
    let listener_discovered = Arc::clone(&discovered);
    thread::spawn(move || {
        run_discovery_listener(listener_device_id, listener_discovered);
    });

    thread::spawn(move || {
        run_hotkey_listener(hotkey_device_id, hotkey_status_map);
    });

    thread::spawn(move || {
        let conn = open_db();
        run_watcher(&conn, watcher_paused, watcher_device_id);
    });

    // One-shot presence announce + one-shot sync at startup, replacing the
    // old 3-second broadcast loop and 2-minute sync interval. After this,
    // everything is triggered by a real event (see NOTE above run_sync_loop's
    // old definition).
    let startup_device_id = device_config.device_id.clone();
    let startup_device_name = device_config.device_name.clone();
    announce_presence_once(startup_device_id, startup_device_name);
    thread::spawn(move || {
        sync_all_peers(&sync_device_id, &sync_status_map);
    });
    // Bidirectional catch-up: also tell every already-authorized peer to
    // pull from us right now, in case we captured clips while they were
    // offline. This is what makes "already connected" devices reconcile
    // immediately instead of waiting for the next event on either side.
    notify_all_peers_to_pull();

    let conn = open_db();
    run_server(conn, paused, device_config.secret, device_config.device_id, device_config.device_name, status_map, discovered, pending);
}

// --- GLOBAL HOTKEY ---

fn run_hotkey_listener(device_id: String, status_map: SyncStatusMap) {
    use device_query::{DeviceQuery, DeviceState, Keycode};

    let device_state = DeviceState::new();
    let mut was_pressed = false;

    log_info!("Hotkey listener started. Press Ctrl+Alt+H to open the clipboard UI.");

    loop {
        let keys: Vec<Keycode> = device_state.get_keys();
        let ctrl = keys.contains(&Keycode::LControl) || keys.contains(&Keycode::RControl);
        let alt = keys.contains(&Keycode::LAlt) || keys.contains(&Keycode::RAlt);
        let h_pressed = keys.contains(&Keycode::H);

        let combo_pressed = ctrl && alt && h_pressed;

        if combo_pressed && !was_pressed {
            open_browser_to_ui(&device_id, &status_map);
        }

        was_pressed = combo_pressed;

        thread::sleep(Duration::from_millis(100));
    }
}

fn open_browser_to_ui(device_id: &str, status_map: &SyncStatusMap) {
    log_info!("Hotkey pressed — opening clipboard UI...");

    // Opening the UI is a real user action, so it's a legitimate moment to
    // both re-announce our presence (in case a peer restarted recently) and
    // sync with everyone we already know about.
    let device_name = load_or_create_device_config().device_name;
    announce_presence_once(device_id.to_string(), device_name);

    let device_id_owned = device_id.to_string();
    let status_map_owned = Arc::clone(status_map);
    thread::spawn(move || {
        sync_all_peers(&device_id_owned, &status_map_owned);
    });

    let result = process::Command::new("cmd")
        .args(["/C", "start", "http://127.0.0.1:8080"])
        .spawn();

    if let Err(e) = result {
        log_warn!("Warning: failed to open browser ({e})");
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

// --- DIGEST GENERATION ---

fn generate_digest(content: &str, language: &str) -> Option<String> {
    let char_count = content.chars().count();
    if char_count < 200 {
        return None;
    }

    if language != "text" && language != "n/a" {
        let line_count = content.lines().count();
        return Some(format!("{} lines of {} code", line_count, language));
    }

    let stopwords: HashSet<&str> = [
        "the", "and", "for", "are", "but", "not", "you", "all", "can", "her",
        "was", "one", "our", "out", "day", "get", "has", "him", "his", "how",
        "man", "new", "now", "old", "see", "two", "way", "who", "boy", "did",
        "its", "let", "put", "say", "she", "too", "use", "that", "with",
        "this", "have", "from", "they", "will", "would", "there", "their",
        "what", "about", "which", "when", "make", "like", "time", "just",
        "into", "than", "then", "them", "these", "some", "could", "your",
    ].into_iter().collect();

    let sentences: Vec<&str> = content
        .split(|c| c == '.' || c == '!' || c == '?' || c == '\n')
        .map(|s| s.trim())
        .filter(|s| s.len() > 15)
        .collect();

    if sentences.is_empty() {
        return None;
    }
    if sentences.len() == 1 {
        return Some(truncate_str(sentences[0], 140));
    }

    let mut word_freq: HashMap<String, u32> = HashMap::new();
    for sentence in &sentences {
        for word in sentence.split_whitespace() {
            let cleaned: String = word.to_lowercase()
                .chars()
                .filter(|c| c.is_alphanumeric())
                .collect();
            if cleaned.len() > 2 && !stopwords.contains(cleaned.as_str()) {
                *word_freq.entry(cleaned).or_insert(0) += 1;
            }
        }
    }

    let mut best_sentence = sentences[0];
    let mut best_score = -1.0_f64;

    for sentence in &sentences {
        let words: Vec<String> = sentence.split_whitespace()
            .map(|w| w.to_lowercase().chars().filter(|c| c.is_alphanumeric()).collect())
            .filter(|w: &String| w.len() > 2)
            .collect();

        if words.is_empty() {
            continue;
        }

        let raw_score: u32 = words.iter().map(|w| *word_freq.get(w).unwrap_or(&0)).sum();
        let normalized = raw_score as f64 / (words.len() as f64).sqrt();

        if normalized > best_score {
            best_score = normalized;
            best_sentence = sentence;
        }
    }

    Some(truncate_str(best_sentence, 140))
}

fn truncate_str(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_chars).collect();
        format!("{}...", truncated.trim_end())
    }
}

// --- WEB SERVER ---

fn run_server(
    conn: Connection,
    paused: Arc<AtomicBool>,
    my_secret: String,
    my_device_id: String,
    my_device_name: String,
    status_map: SyncStatusMap,
    discovered: DiscoveryMap,
    pending: PendingRequestsMap,
) {
    let server = match Server::http("127.0.0.1:8080") {
        Ok(s) => s,
        Err(e) => {
            log_warn!("Error: could not start the web server ({e})");
            log_warn!("Is port 8080 already in use? Maybe another instance is already running.");
            process::exit(1);
        }
    };

    log_info!("Server running at http://127.0.0.1:8080");

    for request in server.incoming_requests() {
        let url = request.url().to_string();
        let method = request.method().clone();

        if url == "/" || url == "/index.html" {
            serve_html(request);
        } else if url == "/api/status" {
            serve_status(&paused, request);
        } else if url == "/api/pause" && method == tiny_http::Method::Post {
            paused.store(true, Ordering::Relaxed);
            log_info!("Capture paused.");
            serve_status(&paused, request);
        } else if url == "/api/resume" && method == tiny_http::Method::Post {
            paused.store(false, Ordering::Relaxed);
            log_info!("Capture resumed.");
            serve_status(&paused, request);
        } else if url == "/api/device" {
            serve_device_info(request);
        } else if url == "/api/discovered" {
            serve_discovered(&discovered, request);
        } else if url == "/api/discover/scan" && method == tiny_http::Method::Post {
            // On-demand replacement for the old always-on broadcaster: the UI
            // calls this once when the user opens the pairing/sync panel.
            announce_presence_once(my_device_id.clone(), my_device_name.clone());
            let response = Response::from_string("{\"status\":\"scanning\"}");
            let _ = request.respond(response);
        } else if url == "/api/pairing/send-request" && method == tiny_http::Method::Post {
            handle_send_pairing_request(request, &discovered, &my_device_id, &my_device_name, &my_secret);
        } else if url == "/api/pairing/request" && method == tiny_http::Method::Post {
            handle_pairing_request(request, &pending);
        } else if url == "/api/pairing/pending" {
            serve_pending_requests(&pending, request);
        } else if url.starts_with("/api/pairing/respond/") && method == tiny_http::Method::Post {
            handle_pairing_respond(request, &url, &pending, &my_device_id, &my_device_name, &my_secret, &conn, &status_map);
        } else if url == "/api/pairing/complete" && method == tiny_http::Method::Post {
            handle_pairing_complete(request, &my_device_id, &conn, &status_map);
        } else if url == "/api/peers" && method == tiny_http::Method::Get {
            serve_list_peers(&status_map, request);
        } else if url == "/api/peers" && method == tiny_http::Method::Post {
            handle_add_peer_request(request);
        } else if url.starts_with("/api/peers/") && url.ends_with("/sync-now") && method == tiny_http::Method::Post {
            handle_sync_now_request(&conn, &my_device_id, &status_map, request, &url);
        } else if url.starts_with("/api/peers/") && method == tiny_http::Method::Delete {
            handle_remove_peer_request(request, &url);
        } else if url == "/api/sync/notify" && method == tiny_http::Method::Post {
            handle_sync_notify(request, my_device_id.clone(), Arc::clone(&status_map));
        } else if url == "/api/sync/delete" && method == tiny_http::Method::Post {
            handle_sync_delete_request(&conn, &my_secret, request);
        } else if url == "/api/sync/pin" && method == tiny_http::Method::Post {
            handle_sync_pin_request(&conn, &my_secret, request);
        } else if url.starts_with("/api/sync/pull") {
            serve_sync_pull(&conn, &my_secret, request, &url);
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

fn serve_discovered(discovered: &DiscoveryMap, request: tiny_http::Request) {
    let now = Local::now().timestamp();
    let map = discovered.lock().unwrap_or_else(|p| p.into_inner());
    let list: Vec<DiscoveredDevice> = map.values()
        .filter(|d| now - d.last_seen <= 15)
        .cloned()
        .collect();
    let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_string());
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let response = Response::from_string(json).with_header(header);
    let _ = request.respond(response);
}

#[derive(Deserialize)]
struct SendRequestBody {
    device_id: String,
}

fn handle_send_pairing_request(mut request: tiny_http::Request, discovered: &DiscoveryMap, my_device_id: &str, my_device_name: &str, my_secret: &str) {
    let mut body = String::new();
    if request.as_reader().read_to_string(&mut body).is_err() {
        let response = Response::from_string("Bad request").with_status_code(400);
        let _ = request.respond(response);
        return;
    }

    let target_id = match serde_json::from_str::<SendRequestBody>(&body) {
        Ok(b) => b.device_id,
        Err(_) => {
            let response = Response::from_string("Bad request").with_status_code(400);
            let _ = request.respond(response);
            return;
        }
    };

    let target = {
        let map = discovered.lock().unwrap_or_else(|p| p.into_inner());
        map.get(&target_id).cloned()
    };

    let target = match target {
        Some(t) => t,
        None => {
            let response = Response::from_string("Device not found").with_status_code(404);
            let _ = request.respond(response);
            return;
        }
    };

    let request_url = format!("http://{}:{}/api/pairing/request", target.ip, target.port);
    let request_body = serde_json::json!({
        "device_id": my_device_id,
        "device_name": my_device_name,
        "port": 8080,
        "secret": my_secret
    }).to_string();

    let send_result = ureq::post(&request_url)
        .set("Content-Type", "application/json")
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .send_string(&request_body);

    match send_result {
        Ok(_) => {
            log_info!("Pairing: sent a connection request to '{}'", target.device_name);
            let response = Response::from_string("{\"status\":\"sent\"}");
            let _ = request.respond(response);
        }
        Err(e) => {
            log_warn!("Pairing: failed to send request to '{}' ({e})", target.device_name);
            let response = Response::from_string("Failed to send request").with_status_code(500);
            let _ = request.respond(response);
        }
    }
}

#[derive(Deserialize)]
struct PairingRequestBody {
    device_id: String,
    device_name: String,
    port: u16,
    secret: String,
}

// Someone else's device is asking to pair WITH us. Their real IP comes from
// the actual TCP connection (remote_addr), never from a claimed field in
// the body, so this can't be spoofed by lying about an IP in the JSON.
fn handle_pairing_request(mut request: tiny_http::Request, pending: &PendingRequestsMap) {
    let remote_ip = request.remote_addr().map(|a| a.ip().to_string()).unwrap_or_default();

    let mut body = String::new();
    if request.as_reader().read_to_string(&mut body).is_err() {
        let response = Response::from_string("Bad request").with_status_code(400);
        let _ = request.respond(response);
        return;
    }

    match serde_json::from_str::<PairingRequestBody>(&body) {
        Ok(req) => {
            let mut map = pending.lock().unwrap_or_else(|p| p.into_inner());
            map.insert(req.device_id.clone(), PendingRequest {
                device_id: req.device_id.clone(),
                device_name: req.device_name.clone(),
                ip: remote_ip,
                port: req.port,
                secret: req.secret,
            });
            log_info!("Pairing: received a connection request from '{}'", req.device_name);
            let response = Response::from_string("{\"status\":\"received\"}");
            let _ = request.respond(response);
        }
        Err(e) => {
            log_warn!("Pairing: bad request body ({e})");
            let response = Response::from_string("Bad request").with_status_code(400);
            let _ = request.respond(response);
        }
    }
}

fn serve_pending_requests(pending: &PendingRequestsMap, request: tiny_http::Request) {
    let map = pending.lock().unwrap_or_else(|p| p.into_inner());
    let list: Vec<PendingRequest> = map.values().cloned().collect();
    let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_string());
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let response = Response::from_string(json).with_header(header);
    let _ = request.respond(response);
}

#[derive(Deserialize)]
struct PairingResponseBody {
    accept: bool,
}

// The user clicked Allow or Deny on a pending request. If allowed: save the
// requester as a peer, then tell THEM we accepted (including OUR secret),
// so their device automatically saves us as a peer too — completing both
// sides without any manual copy/paste.
fn handle_pairing_respond(mut request: tiny_http::Request, url: &str, pending: &PendingRequestsMap, my_device_id: &str, my_device_name: &str, my_secret: &str, conn: &Connection, status_map: &SyncStatusMap) {
    let device_id = url.trim_start_matches("/api/pairing/respond/").to_string();

    let mut body = String::new();
    let _ = request.as_reader().read_to_string(&mut body);
    let accept = serde_json::from_str::<PairingResponseBody>(&body).map(|b| b.accept).unwrap_or(false);

    let maybe_req = {
        let mut map = pending.lock().unwrap_or_else(|p| p.into_inner());
        map.remove(&device_id)
    };

    let req = match maybe_req {
        Some(r) => r,
        None => {
            let response = Response::from_string("Request not found").with_status_code(404);
            let _ = request.respond(response);
            return;
        }
    };

    if accept {
        add_peer(&req.device_name, &req.ip, req.port, &req.secret);
        log_info!("Pairing: accepted connection with '{}'", req.device_name);

        let complete_url = format!("http://{}:{}/api/pairing/complete", req.ip, req.port);
        let complete_body = serde_json::json!({
            "device_id": my_device_id,
            "device_name": my_device_name,
            "port": 8080,
            "secret": my_secret,
            "accepted": true
        }).to_string();

        let send_result = ureq::post(&complete_url)
            .set("Content-Type", "application/json")
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .send_string(&complete_body);

        if let Err(e) = send_result {
            log_warn!("Pairing: failed to notify '{}' of acceptance ({e})", req.device_name);
        }

        // Handshake just completed on our end — sync immediately instead of
        // waiting for the next hotkey press or a timer.
        let peer_list = load_peers();
        if let Some(peer) = peer_list.peers.iter().find(|p| p.name == req.device_name) {
            sync_with_peer(conn, peer, my_device_id, status_map);
            // Bidirectional: also tell them to pull from us right now, so
            // both sides are caught up the moment the connection is live.
            notify_peer_to_pull(peer);
        }
    } else {
        log_info!("Pairing: declined connection request from '{}'", req.device_name);
    }

    let response = Response::from_string("{\"status\":\"ok\"}");
    let _ = request.respond(response);
}

#[derive(Deserialize)]
struct PairingCompleteBody {
    device_name: String,
    port: u16,
    secret: String,
    accepted: bool,
}

// The device we originally requested has approved us — auto-save them as
// a peer using the IP we actually received this from (not a claimed field).
fn handle_pairing_complete(mut request: tiny_http::Request, my_device_id: &str, conn: &Connection, status_map: &SyncStatusMap) {
    let remote_ip = request.remote_addr().map(|a| a.ip().to_string()).unwrap_or_default();

    let mut body = String::new();
    if request.as_reader().read_to_string(&mut body).is_err() {
        let response = Response::from_string("Bad request").with_status_code(400);
        let _ = request.respond(response);
        return;
    }

    match serde_json::from_str::<PairingCompleteBody>(&body) {
        Ok(resp) => {
            if resp.accepted {
                add_peer(&resp.device_name, &remote_ip, resp.port, &resp.secret);
                log_info!("Pairing: '{}' accepted our request — now connected", resp.device_name);

                // Handshake just completed on the requester's end too — sync
                // right away rather than waiting on a timer.
                let peer_list = load_peers();
                if let Some(peer) = peer_list.peers.iter().find(|p| p.name == resp.device_name) {
                    sync_with_peer(conn, peer, my_device_id, status_map);
                    // Bidirectional: tell them to pull from us too.
                    notify_peer_to_pull(peer);
                }
            }
            let response = Response::from_string("{\"status\":\"ok\"}");
            let _ = request.respond(response);
        }
        Err(e) => {
            log_warn!("Pairing: bad completion body ({e})");
            let response = Response::from_string("Bad request").with_status_code(400);
            let _ = request.respond(response);
        }
    }
}

fn handle_sync_notify(request: tiny_http::Request, device_id: String, status_map: SyncStatusMap) {
    thread::spawn(move || {
        sync_all_peers(&device_id, &status_map);
    });

    let response = Response::from_string("{\"status\":\"triggered\"}");
    let _ = request.respond(response);
}

#[derive(Deserialize)]
struct SyncDeleteBody {
    content_hash: String,
    secret: String,
}

// A peer just deleted a clip on their end and is pushing that deletion to us
// live. We remove it locally by content_hash and deliberately do NOT
// re-propagate — that would ping-pong the delete back and forth forever.
fn handle_sync_delete_request(conn: &Connection, my_secret: &str, mut request: tiny_http::Request) {
    let mut body = String::new();
    if request.as_reader().read_to_string(&mut body).is_err() {
        let response = Response::from_string("Bad request").with_status_code(400);
        let _ = request.respond(response);
        return;
    }

    let parsed = match serde_json::from_str::<SyncDeleteBody>(&body) {
        Ok(b) => b,
        Err(_) => {
            let response = Response::from_string("Bad request").with_status_code(400);
            let _ = request.respond(response);
            return;
        }
    };

    if parsed.secret != my_secret {
        log_warn!("Sync: rejected a delete push with an invalid secret");
        let response = Response::from_string("Unauthorized").with_status_code(401);
        let _ = request.respond(response);
        return;
    }

    let row: Result<(String, String), _> = conn.query_row(
        "SELECT content, content_type FROM clips WHERE content_hash = ?1",
        rusqlite::params![parsed.content_hash],
        |r| Ok((r.get(0)?, r.get(1)?)),
    );

    // If we don't have this clip (already deleted, or it never synced to
    // us), there's nothing to do — that's not an error.
    if let Ok((content, content_type)) = row {
        let _ = conn.execute(
            "DELETE FROM clips WHERE content_hash = ?1",
            rusqlite::params![parsed.content_hash],
        );
        if content_type == "image" {
            if let Err(e) = fs::remove_file(&content) {
                log_warn!("Warning: could not delete synced image file {} ({e})", content);
            }
        }
        log_info!("Sync: removed a clip deleted on a peer");
    }

    let response = Response::from_string("{\"status\":\"ok\"}");
    let _ = request.respond(response);
}

#[derive(Deserialize)]
struct SyncPinBody {
    content_hash: String,
    pinned: bool,
    secret: String,
}

// Mirrors handle_sync_delete_request but for pin state. Same rule: apply
// locally, never re-propagate.
fn handle_sync_pin_request(conn: &Connection, my_secret: &str, mut request: tiny_http::Request) {
    let mut body = String::new();
    if request.as_reader().read_to_string(&mut body).is_err() {
        let response = Response::from_string("Bad request").with_status_code(400);
        let _ = request.respond(response);
        return;
    }

    let parsed = match serde_json::from_str::<SyncPinBody>(&body) {
        Ok(b) => b,
        Err(_) => {
            let response = Response::from_string("Bad request").with_status_code(400);
            let _ = request.respond(response);
            return;
        }
    };

    if parsed.secret != my_secret {
        log_warn!("Sync: rejected a pin push with an invalid secret");
        let response = Response::from_string("Unauthorized").with_status_code(401);
        let _ = request.respond(response);
        return;
    }

    let _ = conn.execute(
        "UPDATE clips SET pinned = ?1 WHERE content_hash = ?2",
        rusqlite::params![parsed.pinned, parsed.content_hash],
    );

    let response = Response::from_string("{\"status\":\"ok\"}");
    let _ = request.respond(response);
}

fn serve_device_info(request: tiny_http::Request) {
    let config = load_or_create_device_config();
    let json = serde_json::to_string(&config).unwrap_or_else(|_| "{}".to_string());
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let response = Response::from_string(json).with_header(header);
    let _ = request.respond(response);
}

#[derive(Serialize)]
struct PeerWithStatus {
    name: String,
    ip: String,
    port: u16,
    last_synced_at: Option<String>,
    last_imported: usize,
    last_error: Option<String>,
}

fn serve_list_peers(status_map: &SyncStatusMap, request: tiny_http::Request) {
    let peer_list = load_peers();
    let status = status_map.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    let result: Vec<PeerWithStatus> = peer_list.peers.iter().map(|p| {
        let s = status.get(&p.name);
        PeerWithStatus {
            name: p.name.clone(),
            ip: p.ip.clone(),
            port: p.port,
            last_synced_at: s.and_then(|s| s.last_synced_at.clone()),
            last_imported: s.map(|s| s.last_imported).unwrap_or(0),
            last_error: s.and_then(|s| s.last_error.clone()),
        }
    }).collect();

    let json = serde_json::to_string(&result).unwrap_or_else(|_| "[]".to_string());
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let response = Response::from_string(json).with_header(header);
    let _ = request.respond(response);
}

#[derive(Deserialize)]
struct AddPeerRequest {
    name: String,
    ip: String,
    port: u16,
    secret: String,
}

fn handle_add_peer_request(mut request: tiny_http::Request) {
    let mut body = String::new();
    if request.as_reader().read_to_string(&mut body).is_err() {
        let response = Response::from_string("Failed to read request body").with_status_code(400);
        let _ = request.respond(response);
        return;
    }

    match serde_json::from_str::<AddPeerRequest>(&body) {
        Ok(req) => {
            add_peer(&req.name, &req.ip, req.port, &req.secret);
            let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
            let response = Response::from_string("{\"status\":\"added\"}").with_header(header);
            let _ = request.respond(response);
        }
        Err(e) => {
            log_warn!("Warning: failed to parse add-peer request ({e})");
            let response = Response::from_string("Invalid request body").with_status_code(400);
            let _ = request.respond(response);
        }
    }
}

fn handle_remove_peer_request(request: tiny_http::Request, url: &str) {
    let name_encoded = url.trim_start_matches("/api/peers/");
    let name = urlencoding_decode(name_encoded);
    remove_peer(&name);
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let response = Response::from_string("{\"status\":\"removed\"}").with_header(header);
    let _ = request.respond(response);
}

fn handle_sync_now_request(conn: &Connection, device_id: &str, status_map: &SyncStatusMap, request: tiny_http::Request, url: &str) {
    let trimmed = url.trim_start_matches("/api/peers/").trim_end_matches("/sync-now");
    let name = urlencoding_decode(trimmed);

    let peer_list = load_peers();
    if let Some(peer) = peer_list.peers.iter().find(|p| p.name == name) {
        sync_with_peer(conn, peer, device_id, status_map);
        let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
        let response = Response::from_string("{\"status\":\"synced\"}").with_header(header);
        let _ = request.respond(response);
    } else {
        let response = Response::from_string("Peer not found").with_status_code(404);
        let _ = request.respond(response);
    }
}

fn serve_sync_pull(conn: &Connection, my_secret: &str, request: tiny_http::Request, url: &str) {
    let provided_secret = url.split('?').nth(1)
        .and_then(|qs| qs.split('&').find(|p| p.starts_with("secret=")))
        .map(|p| p.trim_start_matches("secret="))
        .unwrap_or("");

    if provided_secret != my_secret {
        log_warn!("Sync: rejected pull request with an invalid secret");
        let response = Response::from_string("Unauthorized").with_status_code(401);
        let _ = request.respond(response);
        return;
    }

    let mut stmt = match conn.prepare(
        "SELECT content, content_type, device_id, created_at, ocr_text, content_hash, language, is_sensitive, digest
         FROM clips ORDER BY created_at DESC LIMIT 500"
    ) {
        Ok(s) => s,
        Err(e) => {
            log_warn!("Sync: failed to prepare pull query ({e})");
            let response = Response::from_string("[]").with_status_code(500);
            let _ = request.respond(response);
            return;
        }
    };

    let rows: Vec<SyncClip> = stmt.query_map([], |row| {
        Ok(SyncClip {
            content: row.get(0)?,
            content_type: row.get(1)?,
            device_id: row.get(2)?,
            created_at: row.get(3)?,
            ocr_text: row.get(4)?,
            content_hash: row.get(5)?,
            language: row.get(6)?,
            is_sensitive: row.get(7)?,
            digest: row.get(8)?,
        })
    }).map(|iter| iter.filter_map(|r| r.ok()).collect()).unwrap_or_default();

    let json = serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string());
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let response = Response::from_string(json).with_header(header);
    let _ = request.respond(response);
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
            log_warn!("Warning: failed to serialize clips to JSON ({e})");
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

    let row: Result<(String, String, String), _> = conn.query_row(
        "SELECT content, content_type, content_hash FROM clips WHERE id = ?1",
        rusqlite::params![id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    );

    let (content, content_type, content_hash) = match row {
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
                    log_warn!("Warning: could not delete image file {} ({e})", content);
                }
            }
            log_info!("Deleted clip {}", id);

            // Live-propagate to every connected peer right now, instead of
            // waiting for the next pull.
            propagate_delete_to_peers(content_hash);

            let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
            let response = Response::from_string("{\"status\":\"deleted\"}").with_header(header);
            let _ = request.respond(response);
        }
        Err(e) => {
            log_warn!("Error: failed to delete clip {} ({e})", id);
            let response = Response::from_string("Failed to delete").with_status_code(500);
            let _ = request.respond(response);
        }
    }
}

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

    let current: Result<(bool, String), _> = conn.query_row(
        "SELECT pinned, content_hash FROM clips WHERE id = ?1",
        rusqlite::params![id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    );

    let (current_pinned, content_hash) = match current {
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
            // Live-propagate to every connected peer right now.
            propagate_pin_to_peers(content_hash, new_pinned);

            let json = format!("{{\"pinned\":{}}}", new_pinned);
            let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
            let response = Response::from_string(json).with_header(header);
            let _ = request.respond(response);
        }
        Err(e) => {
            log_warn!("Error: failed to toggle pin for clip {} ({e})", id);
            let response = Response::from_string("Failed to update").with_status_code(500);
            let _ = request.respond(response);
        }
    }
}

fn fetch_clips(conn: &Connection, search_query: Option<&str>) -> Vec<ClipJson> {
    let (sql, pattern);
    if let Some(q) = search_query {
        sql = "SELECT id, content, content_type, created_at, ocr_text, language, pinned, is_sensitive, digest
               FROM clips
               WHERE content LIKE ?1 OR ocr_text LIKE ?1
               ORDER BY pinned DESC, created_at DESC LIMIT 100";
        pattern = format!("%{}%", q);
    } else {
        sql = "SELECT id, content, content_type, created_at, ocr_text, language, pinned, is_sensitive, digest
               FROM clips
               ORDER BY pinned DESC, created_at DESC LIMIT 100";
        pattern = String::new();
    }

    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => {
            log_warn!("Warning: failed to prepare query ({e})");
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
            log_warn!("Warning: query failed ({e})");
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
        digest: row.get(8)?,
    })
}

// --- CLI SEARCH ---

fn run_search(conn: &Connection, query: &str) {
    let pattern = format!("%{}%", query);

    let mut stmt = match conn.prepare(
        "SELECT id, content, content_type, created_at, ocr_text, language, pinned, is_sensitive, digest
         FROM clips
         WHERE content LIKE ?1 OR ocr_text LIKE ?1
         ORDER BY pinned DESC, created_at DESC"
    ) {
        Ok(s) => s,
        Err(e) => {
            println!("Error: search query failed to prepare ({e})");
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
            row.get::<_, Option<String>>(8)?,
        ))
    }) {
        Ok(r) => r,
        Err(e) => {
            println!("Error: search failed to run ({e})");
            return;
        }
    };

    println!("Search results for \"{}\":\n", query);

    let mut count = 0;
    for row in results {
        let (id, content, content_type, created_at, ocr_text, language, pinned, is_sensitive, digest) = match row {
            Ok(r) => r,
            Err(e) => {
                println!("Warning: skipping a row that failed to read ({e})");
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
        if let Some(d) = &digest {
            println!("  TL;DR: {}", d);
        }
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

fn run_watcher(conn: &Connection, paused: Arc<AtomicBool>, device_id: String) {
    if let Err(e) = fs::create_dir_all("clip_images") {
        log_warn!("Error: could not create clip_images folder ({e})");
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
                log_warn!("Warning: could not access clipboard (attempt {attempt}/3): {e}");
                thread::sleep(Duration::from_millis(500));
            }
        }
    }

    let mut clipboard = match clipboard {
        Some(cb) => cb,
        None => {
            log_warn!("Error: could not access the clipboard after 3 attempts. Watcher stopping.");
            return;
        }
    };

    let mut last_hash = String::new();

    log_info!("Clipboard watcher started. Writing to clipboard.db ...");

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
                    let digest = generate_digest(&current_text, &language);
                    if save_clip_with_ocr(conn, &current_text, "text", None, &hash, &language, is_sensitive, digest.as_deref(), &device_id) {
                        last_hash = hash;
                        handled_this_tick = true;
                        notify_all_peers_to_pull();
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
                                    let digest = ocr_text
                                        .as_deref()
                                        .and_then(|t| generate_digest(t, "text"));

                                    if save_clip_with_ocr(conn, &filename, "image", ocr_text.as_deref(), &hash, "n/a", is_sensitive, digest.as_deref(), &device_id) {
                                        last_hash = hash;
                                        log_info!("Saved screenshot: {}", filename);
                                        if let Some(text) = &ocr_text {
                                            log_info!("OCR extracted {} chars", text.len());
                                        }
                                        notify_all_peers_to_pull();
                                    }
                                }
                                Err(e) => {
                                    log_warn!("Warning: failed to save screenshot to {} ({e})", filename);
                                    last_hash = hash;
                                }
                            }
                        } else {
                            log_warn!("Warning: clipboard image data didn't match its reported width/height, skipping.");
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
            log_warn!("Warning: OCR could not open image {} ({e})", image_path);
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
            log_warn!("Warning: OCR failed on {} ({e}) — is Tesseract installed and on PATH?", image_path);
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
    digest: Option<&str>,
    device_id: &str,
) -> bool {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    let result = conn.execute(
        "INSERT INTO clips (content, content_type, device_id, created_at, ocr_text, content_hash, language, is_sensitive, digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params![content, content_type, device_id, timestamp, ocr_text, content_hash, language, is_sensitive, digest],
    );

    match result {
        Ok(_) => {
            let flag = if is_sensitive { " [flagged as sensitive]" } else { "" };
            log_info!("Saved {} clip ({}){} at {}", content_type, language, flag, timestamp);
            true
        }
        Err(e) => {
            log_warn!("Error: failed to save clip to database ({e})");
            false
        }
    }
}