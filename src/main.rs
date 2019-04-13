mod arbiter;
mod messages;

use tokio::net::TcpListener;

fn main() {
    let addr = "127.0.0.1:4585".parse().unwrap();
    let listener = TcpListener::bind(&addr)
        .expect("unable to bind TCP listener");
}
