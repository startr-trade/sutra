//! `IntakeWindow`: the bounded set of intake calls a broker source keeps in flight. The window
//! never holds more than its cap, hands each finished call back with its own token in
//! completion order, and drains within a bound.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sutra_channels::{AckDecision, BoxFuture, DeliveryDisposition, IntakeWindow};
use tokio::sync::oneshot;

/// A call that counts how many calls run at once and finishes when `release` fires.
fn gated_call(
    running: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    release: oneshot::Receiver<AckDecision>,
) -> BoxFuture<'static, DeliveryDisposition> {
    Box::pin(async move {
        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(now, Ordering::SeqCst);
        let decision = release.await.unwrap_or(AckDecision::NackRequeue);
        running.fetch_sub(1, Ordering::SeqCst);
        DeliveryDisposition::Settle(decision)
    })
}

/// A source that pushes only while `has_room` never runs more than `max_in_flight` calls.
#[tokio::test]
async fn never_more_than_max_in_flight_calls() {
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut window = IntakeWindow::new(4);
    let mut releases = Vec::new();
    let mut settled = 0usize;
    let mut next_token = 0usize;
    while settled < 20 {
        while window.has_room() && next_token < 20 {
            let (tx, rx) = oneshot::channel();
            releases.push(tx);
            window.push(next_token, gated_call(running.clone(), peak.clone(), rx));
            next_token += 1;
        }
        assert!(window.len() <= 4);
        assert!(!window.has_room() || next_token == 20);
        // Poll every pushed call once (none can finish yet), then release the oldest one.
        assert!(
            tokio::time::timeout(Duration::from_millis(5), window.next())
                .await
                .is_err(),
            "no call finishes before it is released"
        );
        assert_eq!(running.load(Ordering::SeqCst), window.len());
        if let Some(tx) = releases.drain(..1).next() {
            let _ = tx.send(AckDecision::Ack);
        }
        let (_token, disposition) = window.next().await.expect("a call is in flight");
        assert_eq!(disposition, DeliveryDisposition::Settle(AckDecision::Ack));
        settled += 1;
    }
    assert!(window.is_empty());
    assert!(window.next().await.is_none());
    assert_eq!(
        peak.load(Ordering::SeqCst),
        4,
        "the window fills, never overfills"
    );
    assert_eq!(running.load(Ordering::SeqCst), 0);
}

/// Finished calls come back in completion order, each with the token it was pushed with.
#[tokio::test]
async fn finished_calls_return_their_own_token() {
    let mut window = IntakeWindow::new(3);
    let decisions = [
        AckDecision::Ack,
        AckDecision::NackRequeue,
        AckDecision::NackDrop,
    ];
    let mut releases = Vec::new();
    for (token, _) in decisions.iter().enumerate() {
        let (tx, rx) = oneshot::channel::<AckDecision>();
        releases.push(tx);
        window.push(
            token,
            Box::pin(async move {
                DeliveryDisposition::Settle(rx.await.unwrap_or(AckDecision::NackRequeue))
            }),
        );
    }
    assert!(!window.has_room());
    // Release in reverse arrival order: 2, then 0, then 1.
    let mut releases: Vec<Option<_>> = releases.into_iter().map(Some).collect();
    let mut seen = Vec::new();
    for token in [2usize, 0, 1] {
        let _ = releases[token]
            .take()
            .expect("unreleased")
            .send(decisions[token]);
        let (got, disposition) = window.next().await.expect("finished");
        assert_eq!(got, token, "completion order, own token");
        assert_eq!(disposition, DeliveryDisposition::Settle(decisions[token]));
        seen.push(got);
    }
    assert_eq!(seen, vec![2, 0, 1]);

    // A deferred call frees its slot like any other.
    window.push(9, Box::pin(async { DeliveryDisposition::Deferred }));
    assert_eq!(
        window.next().await,
        Some((9, DeliveryDisposition::Deferred))
    );
}

/// `drain` waits for every call in flight and returns them all, but never past its bound.
#[tokio::test(start_paused = true)]
async fn drain_returns_every_finished_call_within_the_bound() {
    let mut window = IntakeWindow::new(8);
    for (token, millis) in [(0usize, 50u64), (1, 10), (2, 200)] {
        window.push(
            token,
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(millis)).await;
                DeliveryDisposition::Settle(AckDecision::Ack)
            }),
        );
    }
    let started = tokio::time::Instant::now();
    let finished = window.drain(Duration::from_secs(30)).await;
    let tokens: Vec<usize> = finished.iter().map(|(token, _)| *token).collect();
    assert_eq!(tokens, vec![1, 0, 2], "every call, completion order");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(window.is_empty());

    // A call that outlives the bound is not returned and stays in the window.
    window.push(
        7,
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(120)).await;
            DeliveryDisposition::Settle(AckDecision::Ack)
        }),
    );
    window.push(
        8,
        Box::pin(async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            DeliveryDisposition::Settle(AckDecision::Ack)
        }),
    );
    let started = tokio::time::Instant::now();
    let finished = window.drain(Duration::from_secs(30)).await;
    assert_eq!(started.elapsed(), Duration::from_secs(30));
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].0, 8);
    assert_eq!(window.len(), 1);
}
