mod arbiter;
mod arbiter_fut;
mod messages;

use log::{error, info};
use messages::*;
use std::rc::Rc;
use std::cell::RefCell;
use tokio::runtime::current_thread::{TaskExecutor, run, spawn};
use tokio::net::TcpListener;
use tokio::prelude::*;
use tower_grpc::{Request, Response, Status};
use tower_h2::Server;

type TID = u64;
type RID = String;
type State = arbiter_fut::ArbiterFut<TID, RID>;

#[derive(Clone)]
struct ArbiterHandler {
    state: Rc<RefCell<State>>
}

impl Into<Error> for arbiter::Error {
    fn into(self) -> Error {
        match self {
            arbiter::Error::UnknownTransaction => Error::UnknownTransaction,
            arbiter::Error::InvalidTransactionState => Error::InvalidTransactionState
        }
    }
}

impl server::Arbiter for ArbiterHandler {
    type StartTransactionFuture = future::FutureResult<Response<StartTransactionResponse>, Status>;
    fn start_transaction(&mut self, request: Request<StartTransactionRequest>) -> Self::StartTransactionFuture {
        let req = request.get_ref();
        let tid = req.tid;
        let mut state = self.state.borrow_mut();
        state.start_transaction(tid);
        let result = StartTransactionResponse {};
        future::ok(Response::new(result))
    }

    type WaitCommitFuture = Box<Future<Item=Response<WaitCommitResponse>, Error=Status>>;
    fn wait_commit(&mut self, request: Request<WaitCommitRequest>) -> Self::WaitCommitFuture {
        let req = request.get_ref();
        let tid = req.tid;
        let mut state = self.state.borrow_mut();
        let fut = state.start_commit(&tid)
            .then(|res| {
                let result = match res {
                    Ok(commit_result) => {
                        let status = match commit_result {
                            arbiter_fut::CommitResult::Proceed => wait_commit_response::CommitStatus::Proceed,
                            arbiter_fut::CommitResult::Retry => wait_commit_response::CommitStatus::Retry
                        };
                        WaitCommitResponse {
                            error: Error::Ok as i32,
                            status: status as i32
                        }
                    },
                    Err(err) => {
                        let error: Error = err.into();
                        WaitCommitResponse {
                            error: error as i32,
                            status: wait_commit_response::CommitStatus::Retry as i32
                        }
                    }
                };
                future::ok(Response::new(result))
            });
        Box::new(fut)
    }
}

fn main() {
    stderrlog::new()
    .verbosity(4)
    .timestamp(stderrlog::Timestamp::Millisecond)
    .init().unwrap();

    let handler = ArbiterHandler {
        state: Rc::new(RefCell::new(
            arbiter_fut::ArbiterFut::new()
        ))
    };

    let new_service = server::ArbiterServer::new(handler);

    let h2_settings = Default::default();
    let mut h2 = Server::new(new_service, h2_settings, TaskExecutor::current());

    let addr = "127.0.0.1:4585".parse().unwrap();
    info!("Listening on {}", addr);
    let listener = TcpListener::bind(&addr).expect("unable to bind TCP listener");

    let server = listener
        .incoming()
        .for_each(move |sock| {
            if let Err(e) = sock.set_nodelay(true) {
                return Err(e);
            }

            let serve = h2.serve(sock);
            spawn(serve.map_err(|e| error!("h2 error: {:?}", e)));

            Ok(())
        })
        .map_err(|e| error!("accept error: {}", e));

    run(server);
}
