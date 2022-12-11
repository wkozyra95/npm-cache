use crate::api::start_server;

mod api;
mod cache;
mod npm;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    start_server().await;
}
