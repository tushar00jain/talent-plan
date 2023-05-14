use std::cmp::max;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{thread, u64};

use labrpc::Error;

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    timestamp: Arc<Mutex<u64>>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(
        &self,
        _: TimestampRequest,
    ) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let mut timestamp = self.timestamp.lock().unwrap();

        *timestamp = max(now, *timestamp + 1);

        Ok(TimestampResponse {
            timestamp: *timestamp,
        })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    #[inline]
    fn get_column(&self, column: Column) -> &BTreeMap<Key, Value> {
        match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        }
    }

    fn get_column_mut(&mut self, column: Column) -> &mut BTreeMap<Key, Value> {
        match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        }
    }

    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        self.get_column(column)
            .range((
                Included((key.clone(), ts_start_inclusive.unwrap())),
                Included((key, ts_end_inclusive.unwrap())),
            ))
            .last()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        self.get_column_mut(column).insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        self.get_column_mut(column).remove_entry(&(key, commit_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        loop {
            let mut table = self.data.lock().unwrap();

            if table
                .read(
                    req.key.clone(),
                    Column::Lock,
                    Some(0),
                    Some(req.start_ts),
                )
                .is_some()
            {
                drop(table);
                self.back_off_maybe_clean_up_lock(
                    req.start_ts,
                    req.key.clone(),
                );
                continue;
            };

            let latest_write = table.read(
                req.key.clone(),
                Column::Write,
                Some(0),
                Some(req.start_ts),
            );

            if latest_write.is_none() {
                return Ok(GetResponse { value: Vec::new() });
            }

            let data_ts = latest_write
                .map(|((_, _), value)| {
                    if let Value::Timestamp(data_ts) = value {
                        return data_ts;
                    }
                    unreachable!();
                })
                .unwrap()
                .clone();

            let value = table
                .read(
                    req.key.clone(),
                    Column::Data,
                    Some(data_ts),
                    Some(data_ts),
                )
                .map(|(_, value)| value)
                .unwrap();

            if let Value::Vector(data) = value {
                return Ok(GetResponse {
                    value: data.clone(),
                });
            };

            unreachable!()
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(
        &self,
        req: PrewriteRequest,
    ) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        let mut table = self.data.lock().unwrap();

        if table
            .read(
                req.key.clone(),
                Column::Write,
                Some(req.start_ts),
                Some(u64::MAX),
            )
            .is_some()
        {
            return Err(Error::Stopped);
        }

        if table
            .read(req.key.clone(), Column::Lock, Some(0), Some(u64::MAX))
            .is_some()
        {
            return Err(Error::Stopped);
        }

        table.write(
            req.key.clone(),
            Column::Data,
            req.start_ts,
            Value::Vector(req.value),
        );
        table.write(
            req.key.clone(),
            Column::Lock,
            req.start_ts,
            Value::Vector(req.primary_key),
        );

        Ok(PrewriteResponse {})
    }

    // example commit RPC handler.
    async fn commit(
        &self,
        req: CommitRequest,
    ) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let mut table = self.data.lock().unwrap();

        if table
            .read(
                req.key.clone(),
                Column::Lock,
                Some(req.start_ts),
                Some(req.start_ts),
            )
            .is_none()
        {
            return Err(Error::Stopped);
        }

        table.write(
            req.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        table.erase(req.key.clone(), Column::Lock, req.start_ts);

        Ok(CommitResponse {})
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        thread::sleep(Duration::from_nanos(TTL));

        let mut table = self.data.lock().unwrap();

        let Some(((_, ts), value)) = table
            .read(key.clone(), Column::Lock, Some(0), Some(start_ts)) else {
                return;
            };

        let Value::Vector(primary_key) = value else {
            unreachable!();
        };

        let ts = ts.clone();

        table.erase(key.clone(), Column::Lock, ts.clone());

        if let Some(((_, _), value)) =
            table.read(key.clone(), Column::Write, Some(0), Some(ts))
        {

            let Value::Timestamp(commit_ts) = value else {
                unreachable!();
            };
            let commit_ts = commit_ts.clone();

            table.write(
                key.clone(),
                Column::Write,
                commit_ts.clone(),
                Value::Timestamp(ts.clone()),
            );
        }
    }
}
