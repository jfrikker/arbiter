mod arbiter;
mod coordinator;
mod messages;

use log::{error, info};
use std::rc::Rc;
use std::cell::RefCell;
use tokio::prelude::*;
use tokio::net::TcpListener;
use tokio::runtime::current_thread::{run, spawn};

type State = Rc<RefCell<coordinator::Coordinator>>;

fn main() {
    stderrlog::new()
    .verbosity(4)
    .timestamp(stderrlog::Timestamp::Millisecond)
    .init().unwrap();

    let arbiter = arbiter::Arbiter::new();
    let coordinator = coordinator::Coordinator::new(arbiter);
    let state: State = Rc::new(RefCell::new(coordinator));

    let addr = "127.0.0.1:4585".parse().unwrap();
    info!("Listening on {}", addr);
    let listener = TcpListener::bind(&addr).expect("unable to bind TCP listener");

    let server = listener.incoming()
        .map_err(|e| error!("Error accepting connections: {}", e))
        .for_each(|sock| {
            Ok(())
        });

    run(server);
}
