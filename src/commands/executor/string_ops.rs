//! String-related command helper methods on CommandExecutor.

use std::time::{Duration, SystemTime};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Value;

use super::CommandExecutor;

impl CommandExecutor {
    #[inline]
    pub(super) fn get(&self, db: u8, key: &Bytes) -> Frame {
        crate::commands::handlers::observe::record_command_access(key.as_ref(), false);
        match self.store.get(db, key) {
            Some(Value::String(data)) => Frame::bulk(data),
            Some(_) => {
                Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            None => Frame::null(),
        }
    }

    #[inline]
    pub(super) fn set(
        &self,
        db: u8,
        key: Bytes,
        value: Bytes,
        options: crate::commands::parser::SetOptions,
    ) -> Frame {
        crate::commands::handlers::observe::record_command_access(key.as_ref(), true);
        // Handle NX/XX conditions
        let exists = self.store.get(db, &key).is_some();

        if options.nx && exists {
            return Frame::null();
        }
        if options.xx && !exists {
            return Frame::null();
        }

        // Get old value if GET option is set, and existing TTL if KEEPTTL
        let (old_value, existing_expiry) = if options.get || options.keep_ttl {
            match self.store.get_entry(db, &key) {
                Some((val, exp)) => (Some(val), exp),
                None => (None, None),
            }
        } else {
            (None, None)
        };

        // Determine expiration: EXAT/PXAT (absolute) > EX/PX (relative) > KEEPTTL
        let expires_at = if let Some(ts_sec) = options.expire_at_sec {
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(ts_sec))
        } else if let Some(ts_ms) = options.expire_at_ms {
            Some(SystemTime::UNIX_EPOCH + Duration::from_millis(ts_ms))
        } else if let Some(expire_ms) = options.expire_ms {
            Some(SystemTime::now() + Duration::from_millis(expire_ms))
        } else if options.keep_ttl {
            existing_expiry
        } else {
            None
        };

        // Set the value with or without expiry
        match expires_at {
            Some(exp) => self
                .store
                .set_with_expiry(db, key, Value::String(value), exp),
            None => self.store.set(db, key, Value::String(value)),
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
        for key in keys {
            crate::commands::handlers::observe::record_command_access(key.as_ref(), true);
        }
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
        for (key, _) in &pairs {
            crate::commands::handlers::observe::record_command_access(key.as_ref(), true);
        }
        for (key, value) in pairs {
            self.store.set(db, key, Value::String(value));
        }
        Frame::simple("OK")
    }
}
