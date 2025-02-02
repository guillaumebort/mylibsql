use std::{future::Future, pin::Pin, task::Poll};

use anyhow::{anyhow, Result};
use futures::FutureExt;
use libsql::replication::FrameNo;
use tokio::sync::oneshot;

/// A future that resolves when an operation is safely checkpointed
#[must_use = "Acks must be awaited, or explicitly dropped if you don't care about the result"]
pub struct Ack<A>
where
    A: Unpin,
{
    result: Option<A>,
    ack: Option<oneshot::Receiver<()>>,
}

impl<A> Ack<A>
where
    A: Unpin,
{
    /// Lookup the result before it's acked
    pub fn peek(&self) -> &A {
        self.result.as_ref().expect("ack was already consumed")
    }

    /// Split the ack into the actual result and the ack future
    /// Useful when you want to use the result immediately and wait for the ack later
    pub fn split(self) -> (A, Ack<()>) {
        (
            self.result.unwrap(),
            Ack {
                result: Some(()),
                ack: self.ack,
            },
        )
    }

    pub(super) fn new_ready(result: A) -> Self {
        Self {
            result: Some(result),
            ack: None,
        }
    }

    pub(super) fn new_pending(result: A, frame_no: FrameNo) -> (Self, PendingAck) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                result: Some(result),
                ack: Some(rx),
            },
            PendingAck(frame_no, tx),
        )
    }
}

impl<A> Future for Ack<A>
where
    A: Unpin,
{
    type Output = A;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if let Some(ack) = self.ack.as_mut() {
            match ack.poll_unpin(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(self.result.take().unwrap()),
                _ => Poll::Pending,
            }
        } else {
            Poll::Ready(self.result.take().unwrap())
        }
    }
}

#[derive(Debug)]
pub struct PendingAck(FrameNo, oneshot::Sender<()>);

impl PendingAck {
    pub fn ack(self) -> Result<()> {
        self.1.send(()).map_err(|_| anyhow!("failed to ack"))
    }

    pub fn is_ready(&self, last_safe_frame_no: FrameNo) -> bool {
        self.0 <= last_safe_frame_no
    }
}

impl PartialEq for PendingAck {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl PartialOrd for PendingAck {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Eq for PendingAck {}

impl Ord for PendingAck {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use std::task::Context;

    use futures::task::noop_waker_ref;

    use super::*;

    #[tokio::test]
    async fn test_ack_ready() {
        let ack = Ack::new_ready(42);
        assert_eq!(ack.peek(), &42);
        let result = ack.await;
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_ack_pending() {
        let (ack, pending_ack) = Ack::new_pending(42, 1);
        assert_eq!(ack.peek(), &42);

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(&waker);
        let mut ack = Box::pin(ack);

        // Poll should return Pending initially
        assert_eq!(Pin::new(&mut ack).poll(&mut cx), Poll::Pending);

        // Send the ack signal
        pending_ack.1.send(()).unwrap();

        // Poll should now return Ready
        assert_eq!(Pin::new(&mut ack).poll(&mut cx), Poll::Ready(42));
    }

    #[tokio::test]
    async fn test_pending_ack_ordering() {
        let ack1 = PendingAck(1, oneshot::channel().0);
        let ack2 = PendingAck(2, oneshot::channel().0);

        assert!(ack1 < ack2);
        assert!(ack2 > ack1);
        assert_eq!(ack1, PendingAck(1, oneshot::channel().0));
    }

    #[tokio::test]
    async fn test_ack_dropped() {
        let (ack, pending_ack) = Ack::new_pending(42, 1);
        drop(ack);
        assert!(pending_ack.ack().is_err());
    }

    #[tokio::test]
    async fn test_ack_multiple_polls() {
        let (ack, pending_ack) = Ack::new_pending(42, 1);
        assert_eq!(ack.peek(), &42);

        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(&waker);
        let mut ack = Box::pin(ack);

        // Poll should return Pending initially
        assert_eq!(Pin::new(&mut ack).poll(&mut cx), Poll::Pending);

        // Poll again should still return Pending
        assert_eq!(Pin::new(&mut ack).poll(&mut cx), Poll::Pending);

        // Send the ack signal
        pending_ack.1.send(()).unwrap();

        // Poll should now return Ready
        assert_eq!(Pin::new(&mut ack).poll(&mut cx), Poll::Ready(42));
    }

    #[tokio::test]
    async fn test_pending_ack_is_ready() {
        let pending_ack = PendingAck(1, oneshot::channel().0);
        assert!(pending_ack.is_ready(1));
        assert!(!pending_ack.is_ready(0));
    }
}
