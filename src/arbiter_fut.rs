use crate::arbiter::{Arbiter, Error, TransactionUpdate};
use tokio::prelude::future::{Either, ok, err};
use log::warn;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use tokio::prelude::*;
use tokio::sync::oneshot::{Sender, channel};

pub struct ArbiterFut<TID, RID> {
    arbiter: Arbiter<TID, RID>,
    waiting: HashMap<TID, Sender<CommitResult>>
}

impl <TID, RID> ArbiterFut<TID, RID>
    where TID: Eq + Hash + Clone + Debug,
          RID: Eq + Hash + Clone + Debug {
    pub fn new() -> Self {
        ArbiterFut {
            arbiter: Arbiter::new(),
            waiting: HashMap::new()
        }
    }

    pub fn start_transaction(&mut self, id: TID) {
        self.arbiter.start_transaction(id.clone());
    }

    pub fn transaction_progress_many<R, W>(&mut self, id: &TID, read: R, written: W) -> Result<bool, Error>
        where R: IntoIterator<Item=RID>,
              W: IntoIterator<Item=RID> {
        if !self.check_cancelled(id)? {
            return Ok(false);
        }

        let update = self.arbiter.transaction_progress_many(id, read, written)?;
        Ok(update.get_failed().contains(id))
    }

    pub fn start_commit(&mut self, id: &TID) -> impl Future<Item=CommitResult, Error=Error> {
        match self.check_cancelled(id) {
            Ok(false) => return Either::A(ok(CommitResult::MustRetry)),
            Err(e) => return Either::A(err(e)),
            _ => {}
        }
        
        match self.arbiter.start_commit(id) {
            Ok(update) => {
                self.handle_update(&update);
                if update.get_can_commit().contains(id) {
                    Either::A(ok(CommitResult::CanCommit))
                } else if update.get_failed().contains(id) {
                    Either::A(ok(CommitResult::MustRetry))
                } else {
                    let (sender, receiver) = channel();
                    self.waiting.insert(id.clone(), sender);
                    let receiver = receiver.map_err(|e| panic!("Unexpected error {:?}", e));
                    Either::B(receiver)
                }
            }
            Err(e) => {
                Either::A(err(e))
            }
        }
    }

    pub fn commit_completed(&mut self, id: &TID) -> Result<(), Error> {
        let update = self.arbiter.commit_completed(id)?;
        self.handle_update(&update);
        Ok(())
    }

    fn handle_update(&mut self, update: &TransactionUpdate<TID>) {
        for id in update.get_can_commit().iter() {
            self.waiting.remove(id).map(|chan| chan
                .send(CommitResult::CanCommit)
                .unwrap_or_else(|e| warn!("Failed to send: {:?}", e)));
        }

        for id in update.get_failed().iter() {
            self.waiting.remove(id).map(|chan| chan
                .send(CommitResult::MustRetry)
                .unwrap_or_else(|e| warn!("Failed to send: {:?}", e)));
        }
    }

    fn check_cancelled(&self, id: &TID) -> Result<bool, Error> {
        self.arbiter.get_state(id)
            .map(|_| true)
            .or_else(|e| match e {
                Error::UnknownTransaction => Ok(false),
                e => Err(e)
            })
    }
}

#[derive(Debug)]
pub enum CommitResult {
    CanCommit,
    MustRetry
}