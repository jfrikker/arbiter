use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

pub struct Arbiter<TID, RID> {
    txns: Vec<TransactionState<TID, RID>>
}

impl <TID, RID> Arbiter<TID, RID> {
    pub fn new() -> Self {
        Arbiter {
            txns: Vec::new()
        }
    }
}

impl <TID, RID> Arbiter<TID, RID>
    where TID: Eq + Clone + Debug,
          RID: Eq + Hash + Clone + Debug {
    pub fn start_transaction(&mut self, id: TID) {
        self.txns.push(TransactionState::new(id));
    }

    pub fn transaction_progress_many<R, W>(&mut self, id: TID, read: HashSet<RID>, written: HashSet<RID>) -> Result<TransactionUpdate<TID>, TID> {
        self.with_transaction(id.clone(), |txn| txn.transaction_progress_many(read, written))?;
        Ok(self.advance_txns())
    }

    pub fn transaction_progress_read(&mut self, id: TID, read: RID) -> Result<TransactionUpdate<TID>, TID> {
        self.with_transaction(id.clone(), |txn| txn.transaction_progress_read(read))?;
        Ok(self.advance_txns())
    }

    pub fn transaction_progress_write(&mut self, id: TID, written: RID) -> Result<TransactionUpdate<TID>, TID> {
        self.with_transaction(id.clone(), |txn| txn.transaction_progress_write(written))?;
        Ok(self.advance_txns())
    }

    pub fn start_commit(&mut self, id: TID) -> Result<TransactionUpdate<TID>, TID> {
        self.with_transaction(id.clone(), |txn| txn.wait_commit())?;
        Ok(self.advance_txns())
    }

    pub fn commit_completed(&mut self, id: TID) -> Result<TransactionUpdate<TID>, TID> {
        self.with_transaction(id.clone(), |txn| txn.commit_completed())?;
        Ok(self.advance_txns())
    }

    fn advance_txns(&mut self) -> TransactionUpdate<TID> {
        let mut result = TransactionUpdate::no_change();
        result.merge(self.advance_impossible_writes());
        result.merge(self.advance_pending_commits());
        
        result
    }

    fn advance_impossible_writes(&mut self) -> TransactionUpdate<TID> {
        let mut committed_writes = HashSet::new();
        let mut result = TransactionUpdate::no_change();
        for txn in self.txns.iter_mut().rev() {
            match txn.state_type {
                TransactionStateType::InProgress => {
                    if txn.is_commit_prevented(&committed_writes) {
                        txn.failed().unwrap();
                        result.mark_failed(txn.id.clone());
                    }
                }
                TransactionStateType::WaitCommit => {
                    if txn.is_commit_prevented(&committed_writes) {
                        txn.failed().unwrap();
                        result.mark_failed(txn.id.clone());
                    }
                }
                _ => {}
            }

            txn.add_committed_writes(&mut committed_writes);
        }

        result
    }

    fn advance_pending_commits(&mut self) -> TransactionUpdate<TID> {
        let mut pending_writes = HashSet::new();
        let mut result = TransactionUpdate::no_change();
        for txn in self.txns.iter_mut() {
            match txn.state_type {
                TransactionStateType::InProgress => {}
                TransactionStateType::WaitCommit => {
                    if txn.try_commit(&pending_writes).unwrap() {
                        result.mark_can_commit(txn.id.clone());
                    }
                }
                _ => {}
            }

            txn.add_pending_writes(&mut pending_writes);
        }

        result
    }

    fn with_transaction<F, R>(&mut self, id: TID, f: F) -> Result<R, TID>
        where F: FnOnce(&mut TransactionState<TID, RID>) -> Result<R, TID> {
        self.txns.iter_mut()
        .find(|t| t.id == id)
        .map_or(Err(ArbiterError::UnknownTransaction(id)), f)
    }
}

struct TransactionState<TID, RID> {
    id: TID,
    state_type: TransactionStateType,
    read: HashSet<RID>,
    written: HashSet<RID>
}

impl <TID, RID> TransactionState<TID, RID>
    where TID: Clone,
          RID: Eq + Hash + Clone {
    fn new(id: TID) -> TransactionState<TID, RID> {
        TransactionState {
            id,
            state_type: TransactionStateType::InProgress,
            read: HashSet::new(),
            written: HashSet::new()
        }
    }

    fn transaction_progress_many<R, W> (&mut self, read: R, written: W) -> Result<(), TID>
        where R: IntoIterator<Item=RID>,
              W: IntoIterator<Item=RID> {
        match self.state_type {
            TransactionStateType::InProgress => {
                self.read.extend(read.into_iter());
                self.written.extend(written.into_iter());
            },
            _ => return Err(ArbiterError::InvalidTransactionState(self.id.clone()))
        }
        Ok(())
    }

    fn transaction_progress_read(&mut self, read: RID) -> Result<(), TID> {
        match self.state_type {
            TransactionStateType::InProgress => {
                self.read.insert(read);
            },
            _ => return Err(ArbiterError::InvalidTransactionState(self.id.clone()))
        }
        Ok(())
    }

    fn transaction_progress_write(&mut self, written: RID) -> Result<(), TID> {
        match self.state_type {
            TransactionStateType::InProgress => {
                self.written.insert(written);
            },
            _ => return Err(ArbiterError::InvalidTransactionState(self.id.clone()))
        }
        Ok(())
    }

    fn wait_commit(&mut self) -> Result<(), TID> {
        match self.state_type {
            TransactionStateType::InProgress => self.state_type = TransactionStateType::WaitCommit,
            _ => return Err(ArbiterError::InvalidTransactionState(self.id.clone()))
        }
        Ok(())
    }

    fn try_commit(&mut self, pending_writes: &HashSet<RID>) -> Result<bool, TID> {
        match self.state_type {
            TransactionStateType::WaitCommit => {
                let can_commit = self.written.is_disjoint(pending_writes);
                if can_commit {
                    self.state_type = TransactionStateType::Committing;
                }
                Ok(can_commit)
            },
            _ => return Err(ArbiterError::InvalidTransactionState(self.id.clone()))
        }
    }

    fn commit_completed(&mut self) -> Result<(), TID> {
        match self.state_type {
            TransactionStateType::Committing => self.state_type = TransactionStateType::Completed,
            _ => return Err(ArbiterError::InvalidTransactionState(self.id.clone()))
        }
        Ok(())
    }

    fn is_commit_prevented(&self, committed_writes: &HashSet<RID>) -> bool {
        match self.state_type {
            TransactionStateType::InProgress => !self.read.is_disjoint(committed_writes),
            TransactionStateType::WaitCommit => !self.read.is_disjoint(committed_writes),
            _ => false
        }
    }

    fn failed(&mut self) -> Result<(), TID> {
        match self.state_type {
            TransactionStateType::Completed => Err(ArbiterError::InvalidTransactionState(self.id.clone())),
            _ => {
                self.state_type = TransactionStateType::Failed;
                Ok(())
            }
        }
    }

    fn add_pending_writes(&self, writes: &mut HashSet<RID>) {
        match self.state_type {
            TransactionStateType::WaitCommit => writes.extend(self.written.iter().cloned()),
            TransactionStateType::Committing => writes.extend(self.written.iter().cloned()),
            _ => {}
        }
    }

    fn add_committed_writes(&self, writes: &mut HashSet<RID>) {
        match self.state_type {
            TransactionStateType::Committing => writes.extend(self.written.iter().cloned()),
            TransactionStateType::Completed => writes.extend(self.written.iter().cloned()),
            _ => {}
        }
    }
}

enum TransactionStateType {
    InProgress,
    WaitCommit,
    Committing,
    Completed,
    Failed
}

#[derive(Debug, PartialEq, Eq)]
pub enum ArbiterError<TID> {
    UnknownTransaction(TID),
    InvalidTransactionState(TID)
}

pub type Result<T, TID> = std::result::Result<T, ArbiterError<TID>>;

#[derive(Debug, PartialEq, Eq)]
pub struct TransactionUpdate<TID> {
    can_commit: Vec<TID>,
    failed: Vec<TID>
}

impl <TID> TransactionUpdate<TID> {
    fn no_change() -> Self {
        TransactionUpdate {
            can_commit: Vec::new(),
            failed: Vec::new()
        }
    }

    fn with_can_commit(id: TID) -> Self {
        let mut result = Self::no_change();
        result.mark_can_commit(id);
        result
    }

    fn with_failed(id: TID) -> Self {
        let mut result = Self::no_change();
        result.mark_failed(id);
        result
    }

    fn mark_can_commit(&mut self, id: TID) {
        self.can_commit.push(id);
    }

    fn mark_failed(&mut self, id: TID) {
        self.failed.push(id);
    }

    fn merge(&mut self, other: Self) {
        self.can_commit.extend(other.can_commit);
        self.failed.extend(other.failed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_txn() {
        let mut arbiter: Arbiter<i32, i32> = Arbiter::new();
        arbiter.start_transaction(1);
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(1, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(1)), arbiter.start_commit(1));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.commit_completed(1));
    }

    #[test]
    fn blocking_txn() {
        let mut arbiter: Arbiter<i32, i32> = Arbiter::new();
        arbiter.start_transaction(1);
        arbiter.start_transaction(2);
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(1, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(1)), arbiter.start_commit(1));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(2, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.start_commit(2));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(2)), arbiter.commit_completed(1));
    }

    #[test]
    fn read_write_conflict_after_start_commit() {
        let mut arbiter: Arbiter<i32, i32> = Arbiter::new();
        arbiter.start_transaction(1);
        arbiter.start_transaction(2);
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(1, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(1)), arbiter.start_commit(1));
        assert_eq!(Ok(TransactionUpdate::with_failed(2)), arbiter.transaction_progress_read(2, 100));
    }
}