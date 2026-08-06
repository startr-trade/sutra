//! A host-side HTTP recorder — the endpoint the async out-of-band suites' outbound HTTP
//! callback channels POST to. The engine container reaches it via `host.docker.internal`
//! (host-gateway); the server binds `0.0.0.0` on a dynamic port and records every request
//! body into a [`Recorder`].
//!
//! Dependency-free (std `TcpListener`): the engine's HTTP sink may keep the connection alive,
//! so each connection loops reading `Content-Length`-delimited requests until it closes.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};

use super::util::Recorder;

/// A running host recorder: its bound port (for the callback host env) and the sink of bodies.
pub struct HostRecorder {
    pub port: u16,
    pub recorder: Recorder,
}

/// Start a recorder on a dynamic host port. Runs an accept loop on a background thread for the
/// life of the process (the listener/thread are never joined — the test binary owns them).
pub fn start_recorder() -> HostRecorder {
    let listener = TcpListener::bind("0.0.0.0:0").expect("bind host recorder");
    let port = listener.local_addr().expect("recorder addr").port();
    let recorder = Recorder::default();
    let accept_recorder = recorder.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let conn_recorder = accept_recorder.clone();
            std::thread::spawn(move || handle_connection(stream, conn_recorder));
        }
    });
    HostRecorder { port, recorder }
}

fn handle_connection(stream: TcpStream, recorder: Recorder) {
    let write_stream = match stream.try_clone() {
        Ok(clone) => clone,
        Err(_) => return,
    };
    let mut reader = BufReader::new(stream);
    loop {
        // Request line.
        let mut request_line = String::new();
        match reader.read_line(&mut request_line) {
            Ok(0) => return, // connection closed
            Ok(_) => {}
            Err(_) => return,
        }
        // Headers until the blank line; capture Content-Length.
        let mut content_length = 0usize;
        loop {
            let mut header = String::new();
            match reader.read_line(&mut header) {
                Ok(0) => return,
                Ok(_) => {}
                Err(_) => return,
            }
            if header == "\r\n" || header == "\n" {
                break;
            }
            let lower = header.to_ascii_lowercase();
            if let Some(value) = lower.strip_prefix("content-length:") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
        // Body.
        let mut body = vec![0u8; content_length];
        if reader.read_exact(&mut body).is_err() {
            return;
        }
        recorder.record(String::from_utf8_lossy(&body).into_owned());
        // Reply 200 with no body; keep the connection open for the next request.
        if (&write_stream)
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .is_err()
        {
            return;
        }
        let _ = (&write_stream).flush();
    }
}
