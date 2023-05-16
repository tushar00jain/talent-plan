use std::{clone, convert::TryInto, ops::Mul, thread, time::Duration};

use futures::{executor, Future};
use labrpc::*;

use crate::{
    msg::{CommitRequest, GetRequest, PrewriteRequest, TimestampRequest},
    service::{TSOClient, TransactionClient},
};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    writes: Vec<(Vec<u8>, Vec<u8>)>,
    start_ts: u64,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            writes: Vec::new(),
            start_ts: 0,
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        executor::block_on(self.tso_client.get_timestamp(&TimestampRequest {}))
            .map(|result| result.timestamp)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        self.start_ts = self.get_timestamp().unwrap();
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        throttled_exponential_backoff(|| {
            executor::block_on(self.txn_client.get(&GetRequest {
                key: key.clone(),
                start_ts: self.start_ts,
            }))
            .map(|result| result.value)
        })
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        self.writes.push((key, value));
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        let (primary_key, _) = &self.writes[0];

        for (key, value) in &self.writes {
            let result = throttled_exponential_backoff(|| {
                executor::block_on(self.txn_client.prewrite(&PrewriteRequest {
                    key: key.clone(),
                    start_ts: self.start_ts,
                    value: value.clone(),
                    primary_key: primary_key.clone(),
                }))
            });

            if result.is_err() {
                return Ok(false);
            }
        }

        let commit_ts = self.get_timestamp()?;

        for (key, value) in &self.writes {
            let is_primary = key == primary_key;

            let result = throttled_exponential_backoff(|| {
                executor::block_on(self.txn_client.commit(&CommitRequest {
                    is_primary,
                    key: key.clone(),
                    start_ts: self.start_ts,
                    commit_ts,
                }))
            });

            if result.is_ok() || !is_primary {
                continue;
            }

            let Err(err) = result else {
                unreachable!()
            };

            match err {
                Error::Other(msg) if msg == "reqhook" => return Ok(false),
                _ => return Err(err),
            }
        }

        Ok(true)
    }
}

fn throttled_exponential_backoff<F, T>(mut f: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    for i in 0.. {
        let result = f();

        if i == RETRY_TIMES || result.is_ok() {
            return result;
        }

        thread::sleep(Duration::from_millis(
            BACKOFF_TIME_MS * 2u64.pow(i.try_into().unwrap()),
        ));
    }

    unreachable!()
}
