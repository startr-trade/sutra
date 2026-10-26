//! One MinIO container per call, removed when the handle drops (and by the atexit reaper if the
//! test process exits without dropping it). Started through the docker CLI and polled on its
//! health endpoint, so this layer needs no async runtime and no client crate.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// MinIO as built by the community, pinned by digest.
pub const MINIO_IMAGE: &str =
    "pgsty/minio@sha256:b6bfe7239bfc83fb90d31612d9704d86039dd714f7904b3f1ad68f211e602372";
/// The root user the container is started with.
pub const MINIO_ACCESS_KEY: &str = "objectstore-test";
/// The root password the container is started with.
pub const MINIO_SECRET_KEY: &str = "objectstore-test-secret";
/// The region every fixture store uses.
pub const MINIO_REGION: &str = "us-east-1";

/// A running MinIO; removed when dropped.
pub struct MinioContainer {
    name: String,
    endpoint: String,
}

impl MinioContainer {
    /// Start one and wait until it answers its health check (panics after 60 s).
    pub fn start() -> MinioContainer {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "sutra-test-minio-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let id = docker(&[
            "run",
            "-d",
            "--name",
            &name,
            "-p",
            "127.0.0.1::9000",
            "-e",
            &format!("MINIO_ROOT_USER={MINIO_ACCESS_KEY}"),
            "-e",
            &format!("MINIO_ROOT_PASSWORD={MINIO_SECRET_KEY}"),
            MINIO_IMAGE,
            "server",
            "/data",
        ]);
        crate::reap_on_exit(id);
        let port = docker(&["port", &name, "9000/tcp"])
            .lines()
            .next()
            .and_then(|l| l.rsplit(':').next())
            .and_then(|p| p.trim().parse::<u16>().ok())
            .expect("the store's host port");
        let container = MinioContainer {
            name,
            endpoint: format!("http://127.0.0.1:{port}"),
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        while !live(port) {
            assert!(Instant::now() < deadline, "MinIO did not come up");
            std::thread::sleep(Duration::from_millis(250));
        }
        container
    }

    /// The store's base URL (`http://127.0.0.1:<port>`).
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The container's name: for `docker network connect` (so containers on a test network can
    /// reach the store by an alias) and `docker exec … mc` (the image carries the MinIO client).
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for MinioContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

/// Whether MinIO's liveness endpoint answers 200.
fn live(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let request = format!(
        "GET /minio/health/live HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut head = [0u8; 12];
    stream.read_exact(&mut head).is_ok() && head.ends_with(b"200")
}

fn docker(args: &[&str]) -> String {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("docker runs");
    assert!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}
