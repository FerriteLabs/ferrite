//! String-related command helper methods on CommandExecutor.

use std::time::{Duration, SystemTime};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Value;

use super::CommandExecutor;

impl CommandExecutor {
    #[inline]
    pub(super) fn get(&self, db: u8, key: &Bytes) -> Frame {
        match self.store.get(db, key) {
            Some(Value::String(data)) => Frame::bulk(data),
            Some(_) => {
                Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            None => Frame::null(),
        }
    }

    #[inline]
    pub(super) fn set(&self, db: u8, key: Bytes, value: Bytes, options: crate::commands::parser::SetOptions) -> Frame {
        // Handle NX/XX conditions
        let exists = self.store.get(db, &key).is_some();

        if options.nx && exists {
            return Frame::null();
        }
        if options.xx && !exists {
            return Frame::null();
        }

        // Get old value if GET option is set
        let old_value = if options.get {
            self.store.get(db, &key)
        } else {
            None
        };

        // Set the value
        if let Some(expire_ms) = options.expire_ms {
            let expires_at = SystemTime::now() + Duration::from_millis(expire_ms);
            self.store
                .set_with_expiry(db, key, Value::String(value), expires_at);
        } else {
            self.store.set(db, key, Value::String(value));
        }

        // Return old value or OK
        if options.get {
            match old_value {
                Some(Value::String(data)) => Frame::bulk(data),
                _ => Frame::null(),
            }
        } else {
            Frame::simple("OK")
        }
    }

    #[inline]
    pub(super) fn del(&self, db: u8, keys: &[Bytes]) -> Frame {
        let count = self.store.del(db, keys);
        Frame::Integer(count)
    }

    #[inline]
    pub(super) fn exists(&self, db: u8, keys: &[Bytes]) -> Frame {
        let count = self.store.exists(db, keys);
        Frame::Integer(count)
    }

    #[inline]
    pub(super) fn mget(&self, db: u8, keys: &[Bytes]) -> Frame {
        let values: Vec<Frame> = keys
            .iter()
            .map(|key| match self.store.get(db, key) {
                Some(Value::String(data)) => Frame::bulk(data),
                _ => Frame::null(),
            })
            .collect();
        Frame::array(values)
    }

    #[inline]
    pub(super) fn mset(&self, db: u8, pairs: Vec<(Bytes, Bytes)>) -> Frame {
        for (key, value) in pairs {
            self.store.set(db, key, Value::String(value));
        }
        Frame::simple("OK")
    }
}
