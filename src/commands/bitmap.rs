//! Bitmap commands implementation
//!
//! Provides Redis bitmap operations on string values.
//! Bits are stored within string values, and each byte can hold 8 bits.

use bytes::Bytes;
use std::sync::Arc;

use crate::protocol::Frame;
use crate::storage::{Store, Value};

/// GETBIT key offset
/// Returns the bit value at offset in the string value stored at key.
pub fn getbit(store: &Arc<Store>, db: u8, key: &Bytes, offset: u64) -> Frame {
    match store.get(db, key) {
        Some(Value::String(data)) => {
            let byte_index = (offset / 8) as usize;
            let bit_index = (offset % 8) as u8;

            if byte_index >= data.len() {
                Frame::Integer(0)
            } else {
                let bit = (data[byte_index] >> (7 - bit_index)) & 1;
                Frame::Integer(bit as i64)
            }
        }
        Some(_) => {
            Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
        }
        None => Frame::Integer(0),
    }
}

/// SETBIT key offset value
/// Sets or clears the bit at offset in the string value stored at key.
/// Returns the original bit value at that offset.
pub fn setbit(store: &Arc<Store>, db: u8, key: &Bytes, offset: u64, value: u8) -> Frame {
    // Validate value is 0 or 1
    if value > 1 {
        return Frame::Error("ERR bit is not an integer or out of range".into());
    }

    // Limit offset to prevent excessive memory allocation
    const MAX_OFFSET: u64 = 512 * 1024 * 1024 * 8; // 512MB * 8 bits
    if offset >= MAX_OFFSET {
        return Frame::Error("ERR bit offset is not an integer or out of range".into());
    }

    let byte_index = (offset / 8) as usize;
    let bit_index = (offset % 8) as u8;

    match store.get(db, key) {
        Some(Value::String(data)) => {
            let mut bytes = data.to_vec();

            // Extend if necessary
            if byte_index >= bytes.len() {
                bytes.resize(byte_index + 1, 0);
            }

            // Get old bit value
            let old_bit = (bytes[byte_index] >> (7 - bit_index)) & 1;

            // Set new bit value
            if value == 1 {
                bytes[byte_index] |= 1 << (7 - bit_index);
            } else {
                bytes[byte_index] &= !(1 << (7 - bit_index));
            }

            store.set(db, key.clone(), Value::String(Bytes::from(bytes)));
            Frame::Integer(old_bit as i64)
        }
        Some(_) => {
            Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
        }
        None => {
            // Create new string with the bit set
            let mut bytes = vec![0u8; byte_index + 1];
            if value == 1 {
                bytes[byte_index] |= 1 << (7 - bit_index);
            }
            store.set(db, key.clone(), Value::String(Bytes::from(bytes)));
            Frame::Integer(0)
        }
    }
}

/// BITCOUNT key [start end [BYTE|BIT]]
/// Count the number of set bits (population counting) in a string.
pub fn bitcount(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    start: Option<i64>,
    end: Option<i64>,
    bit_mode: bool, // true = BIT mode, false = BYTE mode (default)
) -> Frame {
    match store.get(db, key) {
        Some(Value::String(data)) => {
            let len = data.len();
            if len == 0 {
                return Frame::Integer(0);
            }

            if bit_mode {
                // BIT mode - start/end are bit indices
                let bit_len = len * 8;
                let start_bit = normalize_index(start.unwrap_or(0), bit_len as i64) as usize;
                let end_bit = normalize_index(end.unwrap_or(-1), bit_len as i64) as usize;

                if start_bit > end_bit || start_bit >= bit_len {
                    return Frame::Integer(0);
                }

                let mut count = 0i64;
                for bit_idx in start_bit..=end_bit.min(bit_len - 1) {
                    let byte_idx = bit_idx / 8;
                    let bit_offset = bit_idx % 8;
                    if (data[byte_idx] >> (7 - bit_offset)) & 1 == 1 {
                        count += 1;
                    }
                }
                Frame::Integer(count)
            } else {
                // BYTE mode - start/end are byte indices
                let start_idx = normalize_index(start.unwrap_or(0), len as i64) as usize;
                let end_idx = normalize_index(end.unwrap_or(-1), len as i64) as usize;

                if start_idx > end_idx || start_idx >= len {
                    return Frame::Integer(0);
                }

                let count: u32 = data[start_idx..=end_idx.min(len - 1)]
                    .iter()
                    .map(|b| b.count_ones())
                    .sum();
                Frame::Integer(count as i64)
            }
        }
        Some(_) => {
            Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
        }
        None => Frame::Integer(0),
    }
}

/// Normalize a Redis index (negative indices count from end)
fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 {
        (len + idx).max(0)
    } else {
        idx.min(len - 1)
    }
}

/// BITOP operation destkey key [key ...]
/// Perform bitwise operations between strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOperation {
    And,
    Or,
    Xor,
    Not,
}

pub fn bitop(
    store: &Arc<Store>,
    db: u8,
    operation: BitOperation,
    destkey: &Bytes,
    keys: &[Bytes],
) -> Frame {
    // NOT requires exactly one key
    if operation == BitOperation::Not && keys.len() != 1 {
        return Frame::Error("ERR BITOP NOT requires one and only one key".into());
    }

    // Collect all string values (cloned to owned)
    let mut strings: Vec<Vec<u8>> = Vec::new();
    let mut max_len = 0usize;

    for key in keys {
        match store.get(db, key) {
            Some(Value::String(data)) => {
                max_len = max_len.max(data.len());
                strings.push(data.to_vec());
            }
            Some(_) => {
                return Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                )
            }
            None => {
                strings.push(Vec::new());
            }
        }
    }

    if strings.is_empty() {
        store.del(db, std::slice::from_ref(destkey));
        return Frame::Integer(0);
    }

    // Compute the result
    let mut result = vec![0u8; max_len];

    match operation {
        BitOperation::And => {
            // Start with all 1s, then AND with each string
            result.iter_mut().for_each(|b| *b = 0xFF);
            for s in &strings {
                for (i, r) in result.iter_mut().enumerate() {
                    let byte = s.get(i).copied().unwrap_or(0);
                    *r &= byte;
                }
            }
        }
        BitOperation::Or => {
            for s in &strings {
                for (i, r) in result.iter_mut().enumerate() {
                    let byte = s.get(i).copied().unwrap_or(0);
                    *r |= byte;
                }
            }
        }
        BitOperation::Xor => {
            for s in &strings {
                for (i, r) in result.iter_mut().enumerate() {
                    let byte = s.get(i).copied().unwrap_or(0);
                    *r ^= byte;
                }
            }
        }
        BitOperation::Not => {
            let s = &strings[0];
            for (i, r) in result.iter_mut().enumerate() {
                let byte = s.get(i).copied().unwrap_or(0);
                *r = !byte;
            }
        }
    }

    let result_len = result.len() as i64;
    store.set(db, destkey.clone(), Value::String(Bytes::from(result)));
    Frame::Integer(result_len)
}

/// BITPOS key bit [start [end [BYTE|BIT]]]
/// Return the position of the first bit set to 1 or 0 in a string.
pub fn bitpos(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    bit: u8,
    start: Option<i64>,
    end: Option<i64>,
    bit_mode: bool, // true = BIT mode, false = BYTE mode (default)
) -> Frame {
    if bit > 1 {
        return Frame::Error("ERR bit must be 0 or 1".into());
    }

    match store.get(db, key) {
        Some(Value::String(data)) => {
            if data.is_empty() {
                // For empty strings, looking for 0 returns 0, looking for 1 returns -1
                return Frame::Integer(if bit == 0 { 0 } else { -1 });
            }

            let len = data.len();
            let has_range = start.is_some() || end.is_some();

            #[allow(clippy::branches_sharing_code)]
            if bit_mode {
                // BIT mode - start/end are bit indices
                let bit_len = len * 8;
                let start_bit = if let Some(s) = start {
                    normalize_index(s, bit_len as i64) as usize
                } else {
                    0
                };
                let end_bit = if let Some(e) = end {
                    normalize_index(e, bit_len as i64) as usize
                } else {
                    bit_len - 1
                };

                if start_bit > end_bit {
                    return Frame::Integer(-1);
                }

                for bit_idx in start_bit..=end_bit.min(bit_len - 1) {
                    let byte_idx = bit_idx / 8;
                    let bit_offset = bit_idx % 8;
                    let current_bit = (data[byte_idx] >> (7 - bit_offset)) & 1;
                    if current_bit == bit {
                        return Frame::Integer(bit_idx as i64);
                    }
                }
                Frame::Integer(-1)
            } else {
                // BYTE mode
                let start_byte = if let Some(s) = start {
                    normalize_index(s, len as i64) as usize
                } else {
                    0
                };
                let end_byte = if let Some(e) = end {
                    normalize_index(e, len as i64) as usize
                } else {
                    len - 1
                };

                if start_byte > end_byte || start_byte >= len {
                    return Frame::Integer(-1);
                }

                // Look for the bit in the range
                for byte_idx in start_byte..=end_byte.min(len - 1) {
                    let byte = data[byte_idx];
                    for bit_offset in 0..8 {
                        let current_bit = (byte >> (7 - bit_offset)) & 1;
                        if current_bit == bit {
                            return Frame::Integer((byte_idx * 8 + bit_offset) as i64);
                        }
                    }
                }

                // If looking for 0 and no explicit end was given, report position after string
                if bit == 0 && !has_range {
                    return Frame::Integer((len * 8) as i64);
                }

                Frame::Integer(-1)
            }
        }
        Some(_) => {
            Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
        }
        None => {
            // Empty key - looking for 0 returns 0, looking for 1 returns -1
            Frame::Integer(if bit == 0 { 0 } else { -1 })
        }
    }
}

/// BITFIELD subcommand types
#[derive(Debug, Clone)]
pub enum BitFieldSubCommand {
    Get {
        encoding: BitFieldEncoding,
        offset: BitFieldOffset,
    },
    Set {
        encoding: BitFieldEncoding,
        offset: BitFieldOffset,
        value: i64,
    },
    IncrBy {
        encoding: BitFieldEncoding,
        offset: BitFieldOffset,
        increment: i64,
    },
    Overflow(OverflowType),
}

#[derive(Debug, Clone, Copy)]
pub enum BitFieldEncoding {
    Signed(u8),   // i1-i64
    Unsigned(u8), // u1-u63
}

#[derive(Debug, Clone)]
pub enum BitFieldOffset {
    Literal(u64),    // #<offset> - bit offset
    Multiplied(u64), // <offset> - offset multiplied by encoding size
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OverflowType {
    Wrap,
    Sat,
    Fail,
}

impl BitFieldEncoding {
    /// Parse encoding string like "i8", "u16", etc.
    pub fn parse(s: &str) -> Option<Self> {
        if s.is_empty() {
            return None;
        }
        let signed = s.starts_with('i');
        let unsigned = s.starts_with('u');
        if !signed && !unsigned {
            return None;
        }
        let bits: u8 = s[1..].parse().ok()?;
        if signed {
            if !(1..=64).contains(&bits) {
                return None;
            }
            Some(BitFieldEncoding::Signed(bits))
        } else {
            if !(1..=63).contains(&bits) {
                return None;
            }
            Some(BitFieldEncoding::Unsigned(bits))
        }
    }

    fn bits(&self) -> u8 {
        match self {
            BitFieldEncoding::Signed(b) | BitFieldEncoding::Unsigned(b) => *b,
        }
    }
}

impl BitFieldOffset {
    /// Parse offset string like "0", "#0", "100", "#100"
    pub fn parse(s: &str) -> Option<Self> {
        if let Some(stripped) = s.strip_prefix('#') {
            let offset: u64 = stripped.parse().ok()?;
            Some(BitFieldOffset::Multiplied(offset))
        } else {
            let offset: u64 = s.parse().ok()?;
            Some(BitFieldOffset::Literal(offset))
        }
    }

    fn resolve(&self, encoding: &BitFieldEncoding) -> u64 {
        match self {
            BitFieldOffset::Literal(o) => *o,
            BitFieldOffset::Multiplied(o) => *o * encoding.bits() as u64,
        }
    }
}

/// BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP|SAT|FAIL]
pub fn bitfield(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    subcommands: &[BitFieldSubCommand],
) -> Frame {
    // Get or create the string value
    let mut bytes = match store.get(db, key) {
        Some(Value::String(data)) => data.to_vec(),
        Some(_) => {
            return Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            )
        }
        None => Vec::new(),
    };

    let mut results: Vec<Frame> = Vec::new();
    let mut overflow_mode = OverflowType::Wrap;

    for subcmd in subcommands {
        match subcmd {
            BitFieldSubCommand::Overflow(mode) => {
                overflow_mode = *mode;
            }
            BitFieldSubCommand::Get { encoding, offset } => {
                let bit_offset = offset.resolve(encoding);
                let value = get_bits(&bytes, bit_offset, encoding);
                results.push(Frame::Integer(value));
            }
            BitFieldSubCommand::Set {
                encoding,
                offset,
                value,
            } => {
                let bit_offset = offset.resolve(encoding);
                ensure_capacity(&mut bytes, bit_offset, encoding.bits());
                let old_value = get_bits(&bytes, bit_offset, encoding);
                set_bits(&mut bytes, bit_offset, encoding, *value);
                results.push(Frame::Integer(old_value));
            }
            BitFieldSubCommand::IncrBy {
                encoding,
                offset,
                increment,
            } => {
                let bit_offset = offset.resolve(encoding);
                ensure_capacity(&mut bytes, bit_offset, encoding.bits());
                let old_value = get_bits(&bytes, bit_offset, encoding);

                match apply_increment(old_value, *increment, encoding, overflow_mode) {
                    Some(new_value) => {
                        set_bits(&mut bytes, bit_offset, encoding, new_value);
                        results.push(Frame::Integer(new_value));
                    }
                    None => {
                        // FAIL overflow - don't modify, return nil
                        results.push(Frame::Null);
                    }
                }
            }
        }
    }

    // Save modified string if it changed
    if !bytes.is_empty() {
        store.set(db, key.clone(), Value::String(Bytes::from(bytes)));
    }

    Frame::Array(Some(results))
}

/// Read bits from byte array
fn get_bits(bytes: &[u8], bit_offset: u64, encoding: &BitFieldEncoding) -> i64 {
    let bits = encoding.bits() as u64;
    let mut value: u64 = 0;

    for i in 0..bits {
        let byte_idx = ((bit_offset + i) / 8) as usize;
        let bit_idx = ((bit_offset + i) % 8) as u8;

        if byte_idx < bytes.len() {
            let bit = (bytes[byte_idx] >> (7 - bit_idx)) & 1;
            value = (value << 1) | (bit as u64);
        } else {
            value <<= 1;
        }
    }

    // Sign extend if signed
    match encoding {
        BitFieldEncoding::Signed(b) => {
            let bits = *b as u64;
            if bits < 64 && (value >> (bits - 1)) & 1 == 1 {
                // Sign extend
                let mask = !((1u64 << bits) - 1);
                (value | mask) as i64
            } else {
                value as i64
            }
        }
        BitFieldEncoding::Unsigned(_) => value as i64,
    }
}

/// Write bits to byte array
fn set_bits(bytes: &mut [u8], bit_offset: u64, encoding: &BitFieldEncoding, value: i64) {
    let bits = encoding.bits() as u64;
    let value = value as u64;

    for i in 0..bits {
        let byte_idx = ((bit_offset + i) / 8) as usize;
        let bit_idx = ((bit_offset + i) % 8) as u8;
        let bit_value = ((value >> (bits - 1 - i)) & 1) as u8;

        if byte_idx < bytes.len() {
            if bit_value == 1 {
                bytes[byte_idx] |= 1 << (7 - bit_idx);
            } else {
                bytes[byte_idx] &= !(1 << (7 - bit_idx));
            }
        }
    }
}

/// Ensure byte array has enough capacity
fn ensure_capacity(bytes: &mut Vec<u8>, bit_offset: u64, bits: u8) {
    let end_bit = bit_offset + bits as u64;
    let needed_bytes = end_bit.div_ceil(8) as usize;
    if bytes.len() < needed_bytes {
        bytes.resize(needed_bytes, 0);
    }
}

/// Apply increment with overflow handling
fn apply_increment(
    value: i64,
    increment: i64,
    encoding: &BitFieldEncoding,
    overflow: OverflowType,
) -> Option<i64> {
    let bits = encoding.bits();

    match encoding {
        BitFieldEncoding::Signed(b) => {
            let min = if *b == 64 {
                i64::MIN
            } else {
                -(1i64 << (b - 1))
            };
            let max = if *b == 64 {
                i64::MAX
            } else {
                (1i64 << (b - 1)) - 1
            };

            match overflow {
                OverflowType::Wrap => {
                    let result = value.wrapping_add(increment);
                    // Wrap within range
                    let range = if bits == 64 { u64::MAX } else { 1u64 << bits };
                    let wrapped = ((result as u64) % range) as i64;
                    // Sign extend
                    if bits < 64 && (wrapped as u64 >> (bits - 1)) & 1 == 1 {
                        Some(wrapped | ((-1i64) << bits))
                    } else if bits < 64 {
                        Some(wrapped & ((1i64 << bits) - 1))
                    } else {
                        Some(wrapped)
                    }
                }
                OverflowType::Sat => {
                    let result = value.saturating_add(increment);
                    Some(result.clamp(min, max))
                }
                OverflowType::Fail => {
                    let result = value.checked_add(increment)?;
                    if result < min || result > max {
                        None
                    } else {
                        Some(result)
                    }
                }
            }
        }
        BitFieldEncoding::Unsigned(b) => {
            let max = if *b >= 63 {
                u64::MAX >> 1
            } else {
                (1u64 << b) - 1
            };

            match overflow {
                OverflowType::Wrap => {
                    let result = (value as u64).wrapping_add(increment as u64);
                    let wrapped = result & max;
                    Some(wrapped as i64)
                }
                OverflowType::Sat => {
                    let result = if increment >= 0 {
                        (value as u64).saturating_add(increment as u64)
                    } else {
                        (value as u64).saturating_sub((-increment) as u64)
                    };
                    Some(result.min(max) as i64)
                }
                OverflowType::Fail => {
                    let result = if increment >= 0 {
                        (value as u64).checked_add(increment as u64)?
                    } else {
                        (value as u64).checked_sub((-increment) as u64)?
                    };
                    if result > max {
                        None
                    } else {
                        Some(result as i64)
                    }
                }
            }
        }
    }
}

/// BITFIELD_RO key GET encoding offset [GET encoding offset ...]
/// Read-only version of BITFIELD (only supports GET subcommand)
pub fn bitfield_ro(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    subcommands: &[BitFieldSubCommand],
) -> Frame {
    let bytes = match store.get(db, key) {
        Some(Value::String(data)) => data.to_vec(),
        Some(_) => {
            return Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            )
        }
        None => Vec::new(),
    };

    let mut results: Vec<Frame> = Vec::new();

    for subcmd in subcommands {
        match subcmd {
            BitFieldSubCommand::Get { encoding, offset } => {
                let bit_offset = offset.resolve(encoding);
                let value = get_bits(&bytes, bit_offset, encoding);
                results.push(Frame::Integer(value));
            }
            _ => return Frame::Error("ERR BITFIELD_RO only supports the GET subcommand".into()),
        }
    }

    Frame::Array(Some(results))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_getbit_setbit() {
        let store = create_store();
        let key = Bytes::from("mykey");

        // Get bit from non-existent key
        let result = getbit(&store, 0, &key, 7);
        assert!(matches!(result, Frame::Integer(0)));

        // Set bit
        let result = setbit(&store, 0, &key, 7, 1);
        assert!(matches!(result, Frame::Integer(0))); // Old value was 0

        // Get the bit we just set
        let result = getbit(&store, 0, &key, 7);
        assert!(matches!(result, Frame::Integer(1)));

        // Set another bit
        let result = setbit(&store, 0, &key, 0, 1);
        assert!(matches!(result, Frame::Integer(0)));

        // Clear the first bit
        let result = setbit(&store, 0, &key, 7, 0);
        assert!(matches!(result, Frame::Integer(1))); // Old value was 1

        let result = getbit(&store, 0, &key, 7);
        assert!(matches!(result, Frame::Integer(0)));
    }

    #[test]
    fn test_bitcount() {
        let store = create_store();
        let key = Bytes::from("mykey");

        // Set some bits to create pattern
        store.set(
            0,
            key.clone(),
            Value::String(Bytes::from_static(&[0xff, 0xf0, 0x00])),
        );

        // Count all bits
        let result = bitcount(&store, 0, &key, None, None, false);
        assert!(matches!(result, Frame::Integer(12))); // 8 + 4 + 0

        // Count bits in first byte
        let result = bitcount(&store, 0, &key, Some(0), Some(0), false);
        assert!(matches!(result, Frame::Integer(8)));

        // Count bits in second byte
        let result = bitcount(&store, 0, &key, Some(1), Some(1), false);
        assert!(matches!(result, Frame::Integer(4)));
    }

    #[test]
    fn test_bitop_and() {
        let store = create_store();
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let dest = Bytes::from("dest");

        store.set(
            0,
            key1.clone(),
            Value::String(Bytes::from_static(&[0xff, 0x0f])),
        );
        store.set(
            0,
            key2.clone(),
            Value::String(Bytes::from_static(&[0xf0, 0xff])),
        );

        let result = bitop(&store, 0, BitOperation::And, &dest, &[key1, key2]);
        assert!(matches!(result, Frame::Integer(2)));

        if let Some(Value::String(data)) = store.get(0, &dest) {
            assert_eq!(data.as_ref(), &[0xf0, 0x0f]);
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_bitop_or() {
        let store = create_store();
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let dest = Bytes::from("dest");

        store.set(
            0,
            key1.clone(),
            Value::String(Bytes::from_static(&[0xf0, 0x00])),
        );
        store.set(
            0,
            key2.clone(),
            Value::String(Bytes::from_static(&[0x0f, 0xff])),
        );

        let result = bitop(&store, 0, BitOperation::Or, &dest, &[key1, key2]);
        assert!(matches!(result, Frame::Integer(2)));

        if let Some(Value::String(data)) = store.get(0, &dest) {
            assert_eq!(data.as_ref(), &[0xff, 0xff]);
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_bitop_not() {
        let store = create_store();
        let key = Bytes::from("key");
        let dest = Bytes::from("dest");

        store.set(
            0,
            key.clone(),
            Value::String(Bytes::from_static(&[0xf0, 0x0f])),
        );

        let result = bitop(&store, 0, BitOperation::Not, &dest, &[key]);
        assert!(matches!(result, Frame::Integer(2)));

        if let Some(Value::String(data)) = store.get(0, &dest) {
            assert_eq!(data.as_ref(), &[0x0f, 0xf0]);
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_bitpos() {
        let store = create_store();
        let key = Bytes::from("mykey");

        store.set(
            0,
            key.clone(),
            Value::String(Bytes::from_static(&[0x00, 0xff, 0xf0])),
        );

        // Find first 1 bit
        let result = bitpos(&store, 0, &key, 1, None, None, false);
        assert!(matches!(result, Frame::Integer(8))); // First 1 is at bit 8

        // Find first 0 bit
        let result = bitpos(&store, 0, &key, 0, None, None, false);
        assert!(matches!(result, Frame::Integer(0))); // First 0 is at bit 0
    }

    #[test]
    fn test_bitfield_get_set() {
        let store = create_store();
        let key = Bytes::from("mykey");

        // SET i8 at offset 0 to value 100
        let subcommands = vec![BitFieldSubCommand::Set {
            encoding: BitFieldEncoding::Signed(8),
            offset: BitFieldOffset::Literal(0),
            value: 100,
        }];
        let result = bitfield(&store, 0, &key, &subcommands);
        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 1);
            assert!(matches!(arr[0], Frame::Integer(0))); // Old value was 0
        }

        // GET i8 at offset 0
        let subcommands = vec![BitFieldSubCommand::Get {
            encoding: BitFieldEncoding::Signed(8),
            offset: BitFieldOffset::Literal(0),
        }];
        let result = bitfield(&store, 0, &key, &subcommands);
        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 1);
            assert!(matches!(arr[0], Frame::Integer(100)));
        }
    }

    #[test]
    fn test_bitfield_incrby() {
        let store = create_store();
        let key = Bytes::from("mykey");

        // SET then INCRBY
        let subcommands = vec![
            BitFieldSubCommand::Set {
                encoding: BitFieldEncoding::Unsigned(8),
                offset: BitFieldOffset::Literal(0),
                value: 10,
            },
            BitFieldSubCommand::IncrBy {
                encoding: BitFieldEncoding::Unsigned(8),
                offset: BitFieldOffset::Literal(0),
                increment: 5,
            },
        ];
        let result = bitfield(&store, 0, &key, &subcommands);
        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 2);
            assert!(matches!(arr[0], Frame::Integer(0))); // Old value
            assert!(matches!(arr[1], Frame::Integer(15))); // After increment
        }
    }
}
