use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

/// Owned dynamically-typed future that is `Send` when `T: Send`.
///
/// Unlike `futures::future::BoxFuture`, the inner future is required to be
/// `Send`, so this type is **safely** `Send` without `unsafe impl`.
pub struct BoxFuture<'a, T>(Pin<Box<dyn Future<Output = T> + Send + 'a>>);

impl<'a, T> BoxFuture<'a, T> {
    pub fn new<F>(f: F) -> BoxFuture<'a, T>
    where
        F: IntoFuture<Output = T> + 'a,
        F::IntoFuture: Send,
    {
        BoxFuture(Box::pin(f.into_future()))
    }

    #[allow(unused)]
    pub fn wrap(f: Pin<Box<dyn Future<Output = T> + Send + 'a>>) -> BoxFuture<'a, T> {
        BoxFuture(f)
    }
}

// Safety: not needed — the `Send` bound on the inner future makes
// `BoxFuture` automatically `Send` when `T: Send`.

impl<T> Future for BoxFuture<'_, T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(context)
    }
}

pub type Runner = BoxFuture<'static, std::io::Result<()>>;
