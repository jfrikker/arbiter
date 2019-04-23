mod arbiter;
mod arbiter_fut;
mod messages;

use log::{error, info};
use std::rc::Rc;
use std::cell::RefCell;
use tokio::prelude::*;
use tokio::codec::Framed;
use tokio::codec::length_delimited::Builder;
use tokio::net::TcpListener;
use tokio::runtime::current_thread::{run, spawn};
use tokio::sync::mpsc::unbounded_channel;
/*
type State = Rc<RefCell<coordinator::Coordinator>>;

fn main() {
    stderrlog::new()
    .verbosity(4)
    .timestamp(stderrlog::Timestamp::Millisecond)
    .init().unwrap();

    let arbiter = arbiter::Arbiter::new();
    let coordinator = coordinator::Coordinator::new(arbiter);
    let state: State = Rc::new(RefCell::new(coordinator));
    let codec = Builder::new()
        .big_endian()
        .length_field_length(4)
        .new_codec();

    let addr = "127.0.0.1:4585".parse().unwrap();
    info!("Listening on {}", addr);
    let listener = TcpListener::bind(&addr).expect("unable to bind TCP listener");

    let server = listener.incoming()
        .map_err(|e| error!("Error accepting connections: {}", e))
        .for_each(|sock| {
            let (write, read) = Framed::new(sock, codec).split();
            let (sender, receiver) = unbounded_channel();

            let out = write
                .sink_map_err(|e| error!("{}", e))
                .send_all(receiver.map_err(|e| error!("{}", e)))
                .map(|_| ());
            spawn(out);

            let sid = state.borrow_mut().start_session(sender);
            read.for_each(|message| {
                state.borrow_mut().handle_message(sid, message);
                Ok(())
            });
            Ok(())
        });

    run(server);
}
*/