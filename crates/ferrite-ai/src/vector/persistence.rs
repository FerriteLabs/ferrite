//! Index persistence — serialize/deserialize HNSW index to disk
//!
//! Custom binary format with:
//! - File header (magic, version, dimensions, metric, element count)
//! - CRC32 checksums per section for corruption detection
//! - Recovery from corrupt files (rebuild from source vectors)

use super::hnsw::HnswIndex;
use super::types::{DistanceMetric, VectorId};
use super::{VectorError, VectorIndex};
use crc32fast::Hasher as Crc32Hasher;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use tracing::{debug, info, warn};

/// Magic bytes identifying a Ferrite HNSW index file
const MAGIC: &[u8; 8] = b"FRTEHNSW";

/// Current file format version
const FORMAT_VERSION: u32 = 1;

/// Section tags
const SECTION_HEADER: u8 = 0x01;
const SECTION_VECTORS: u8 = 0x02;
const SECTION_GRAPH: u8 = 0x03;
const SECTION_MAPPINGS: u8 = 0x04;

/// File header for a persisted HNSW index
#[derive(Debug, Clone)]
pub struct IndexFileHeader {
    /// Format version
    pub version: u32,
    /// Vector dimension
    pub dimension: u32,
    /// Distance metric (0=Cosine, 1=Euclidean, 2=DotProduct, 3=Manhattan)
    pub metric: u8,
    /// Number of elements
    pub element_count: u64,
    /// HNSW M parameter
    pub m: u32,
    /// HNSW ef_construction
    pub ef_construction: u32,
    /// Number of layers
    pub num_layers: u32,
    /// Max level
    pub max_level: u32,
}

fn metric_to_u8(metric: DistanceMetric) -> u8 {
    match metric {
        DistanceMetric::Cosine => 0,
        DistanceMetric::Euclidean => 1,
        DistanceMetric::DotProduct => 2,
        DistanceMetric::Manhattan => 3,
    }
}

fn u8_to_metric(v: u8) -> Result<DistanceMetric, VectorError> {
    match v {
        0 => Ok(DistanceMetric::Cosine),
        1 => Ok(DistanceMetric::Euclidean),
        2 => Ok(DistanceMetric::DotProduct),
        3 => Ok(DistanceMetric::Manhattan),
        _ => Err(VectorError::InvalidConfig(format!(
            "unknown metric type: {}",
            v
        ))),
    }
}

/// Write a u32 in little-endian
fn write_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

/// Write a u64 in little-endian
fn write_u64(w: &mut impl Write, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

/// Read a u32 in little-endian
fn read_u32(r: &mut impl Read) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

/// Read a u64 in little-endian
fn read_u64(r: &mut impl Read) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

/// Write a CRC32-protected section: [tag(1)] [len(4)] [data...] [crc32(4)]
fn write_section(w: &mut impl Write, tag: u8, data: &[u8]) -> io::Result<()> {
    w.write_all(&[tag])?;
    write_u32(w, data.len() as u32)?;
    w.write_all(data)?;
    let mut hasher = Crc32Hasher::new();
    hasher.update(data);
    write_u32(w, hasher.finalize())?;
    Ok(())
}

/// Read a CRC32-protected section. Returns (tag, data).
fn read_section(r: &mut impl Read) -> Result<(u8, Vec<u8>), VectorError> {
    let mut tag_buf = [0u8; 1];
    r.read_exact(&mut tag_buf)
        .map_err(|e| VectorError::Internal(format!("read section tag: {}", e)))?;
    let tag = tag_buf[0];
    let len = read_u32(r).map_err(|e| VectorError::Internal(format!("read section len: {}", e)))?
        as usize;
    let mut data = vec![0u8; len];
    r.read_exact(&mut data)
        .map_err(|e| VectorError::Internal(format!("read section data: {}", e)))?;
    let stored_crc =
        read_u32(r).map_err(|e| VectorError::Internal(format!("read section crc: {}", e)))?;

    let mut hasher = Crc32Hasher::new();
    hasher.update(&data);
    let computed_crc = hasher.finalize();

    if stored_crc != computed_crc {
        return Err(VectorError::Internal(format!(
            "CRC32 mismatch in section 0x{:02x}: stored={:#010x}, computed={:#010x}",
            tag, stored_crc, computed_crc
        )));
    }

    Ok((tag, data))
}

/// Serialize an HNSW index to a writer
pub fn serialize_hnsw<W: Write>(index: &HnswIndex, mut w: W) -> Result<(), VectorError> {
    let map_err = |e: io::Error| VectorError::Internal(format!("write error: {}", e));
    let buf_write_err = |e: io::Error| VectorError::Internal(format!("buffer write error: {}", e));

    // Write magic
    w.write_all(MAGIC).map_err(map_err)?;

    // Build header section
    let header = {
        let mut buf = Vec::new();
        write_u32(&mut buf, FORMAT_VERSION).map_err(buf_write_err)?;
        write_u32(&mut buf, index.dimension() as u32).map_err(buf_write_err)?;
        buf.push(metric_to_u8(index.metric()));
        write_u64(&mut buf, index.len() as u64).map_err(buf_write_err)?;
        write_u32(&mut buf, index.m_param()).map_err(buf_write_err)?;
        write_u32(&mut buf, index.ef_construction_param()).map_err(buf_write_err)?;
        write_u32(&mut buf, index.num_layers() as u32).map_err(buf_write_err)?;
        write_u32(&mut buf, index.max_level_val() as u32).map_err(buf_write_err)?;
        // Entry point internal ID (u32::MAX = none)
        let ep = index.entry_point_id().map(|v| v as u32).unwrap_or(u32::MAX);
        write_u32(&mut buf, ep).map_err(buf_write_err)?;
        buf
    };
    write_section(&mut w, SECTION_HEADER, &header).map_err(map_err)?;
    debug!("wrote header section ({} bytes)", header.len());

    // Build vectors section: [count(4)] [for each: internal_id(4), dim floats as le bytes]
    let vectors_data = {
        let raw = index.raw_vectors();
        let mut buf = Vec::new();
        let mut count: u32 = 0;
        // Reserve space for count
        write_u32(&mut buf, 0).map_err(buf_write_err)?;
        for (i, v) in raw.iter().enumerate() {
            if let Some(vec) = v {
                count += 1;
                write_u32(&mut buf, i as u32).map_err(buf_write_err)?;
                for &val in vec {
                    buf.extend_from_slice(&val.to_le_bytes());
                }
            }
        }
        // Patch count
        buf[0..4].copy_from_slice(&count.to_le_bytes());
        buf
    };
    write_section(&mut w, SECTION_VECTORS, &vectors_data).map_err(map_err)?;
    debug!("wrote vectors section ({} bytes)", vectors_data.len());

    // Build graph section: [num_layers(4)] [for each layer: num_nodes(4) [node(4) num_neighbors(4) [neighbor(4)...]...]]
    let graph_data = {
        let layers = index.raw_layers();
        let mut buf = Vec::new();
        write_u32(&mut buf, layers.len() as u32).map_err(buf_write_err)?;
        for layer in &layers {
            write_u32(&mut buf, layer.len() as u32).map_err(buf_write_err)?;
            for (&node, neighbors) in layer {
                write_u32(&mut buf, node as u32).map_err(buf_write_err)?;
                write_u32(&mut buf, neighbors.len() as u32).map_err(buf_write_err)?;
                for &n in neighbors {
                    write_u32(&mut buf, n as u32).map_err(buf_write_err)?;
                }
            }
        }
        buf
    };
    write_section(&mut w, SECTION_GRAPH, &graph_data).map_err(map_err)?;
    debug!("wrote graph section ({} bytes)", graph_data.len());

    // Build mappings section: [count(4)] [for each: internal_id(4) level(4) id_len(4) id_bytes...]
    let mappings_data = {
        let mappings = index.raw_id_mappings();
        let node_levels = index.raw_node_levels();
        let mut buf = Vec::new();
        write_u32(&mut buf, mappings.len() as u32).map_err(buf_write_err)?;
        for (internal_id, external_id) in &mappings {
            write_u32(&mut buf, *internal_id as u32).map_err(buf_write_err)?;
            let level = node_levels.get(internal_id).copied().unwrap_or(0);
            write_u32(&mut buf, level as u32).map_err(buf_write_err)?;
            let id_bytes = external_id.as_str().as_bytes();
            write_u32(&mut buf, id_bytes.len() as u32).map_err(buf_write_err)?;
            buf.extend_from_slice(id_bytes);
        }
        buf
    };
    write_section(&mut w, SECTION_MAPPINGS, &mappings_data).map_err(map_err)?;
    debug!("wrote mappings section ({} bytes)", mappings_data.len());

    info!(
        "serialized HNSW index: {} vectors, {} dimensions",
        index.len(),
        index.dimension()
    );
    Ok(())
}

/// Deserialize an HNSW index from a reader
pub fn deserialize_hnsw<R: Read>(mut r: R) -> Result<HnswIndex, VectorError> {
    // Read and verify magic
    let mut magic_buf = [0u8; 8];
    r.read_exact(&mut magic_buf)
        .map_err(|e| VectorError::Internal(format!("read magic: {}", e)))?;
    if &magic_buf != MAGIC {
        return Err(VectorError::Internal("invalid magic bytes".to_string()));
    }

    // Read header section
    let (tag, header_data) = read_section(&mut r)?;
    if tag != SECTION_HEADER {
        return Err(VectorError::Internal(format!(
            "expected header section (0x{:02x}), got 0x{:02x}",
            SECTION_HEADER, tag
        )));
    }

    let mut cursor = &header_data[..];
    let version =
        read_u32(&mut cursor).map_err(|e| VectorError::Internal(format!("read version: {}", e)))?;
    if version != FORMAT_VERSION {
        return Err(VectorError::InvalidConfig(format!(
            "unsupported format version: {} (expected {})",
            version, FORMAT_VERSION
        )));
    }
    let dimension = read_u32(&mut cursor)
        .map_err(|e| VectorError::Internal(format!("read dimension: {}", e)))?
        as usize;
    let mut metric_buf = [0u8; 1];
    cursor
        .read_exact(&mut metric_buf)
        .map_err(|e| VectorError::Internal(format!("read metric: {}", e)))?;
    let metric = u8_to_metric(metric_buf[0])?;
    let element_count = read_u64(&mut cursor)
        .map_err(|e| VectorError::Internal(format!("read element_count: {}", e)))?;
    let m = read_u32(&mut cursor).map_err(|e| VectorError::Internal(format!("read m: {}", e)))?
        as usize;
    let ef_construction = read_u32(&mut cursor)
        .map_err(|e| VectorError::Internal(format!("read ef_construction: {}", e)))?
        as usize;
    let _num_layers = read_u32(&mut cursor)
        .map_err(|e| VectorError::Internal(format!("read num_layers: {}", e)))?;
    let max_level = read_u32(&mut cursor)
        .map_err(|e| VectorError::Internal(format!("read max_level: {}", e)))?
        as usize;
    let entry_point_raw = read_u32(&mut cursor)
        .map_err(|e| VectorError::Internal(format!("read entry_point: {}", e)))?;
    let stored_entry_point = if entry_point_raw == u32::MAX {
        None
    } else {
        Some(entry_point_raw as usize)
    };

    debug!(
        "header: dim={}, metric={}, count={}, m={}, ef_c={}",
        dimension, metric, element_count, m, ef_construction
    );

    // Read vectors section
    let (tag, vectors_data) = read_section(&mut r)?;
    if tag != SECTION_VECTORS {
        return Err(VectorError::Internal(
            "expected vectors section".to_string(),
        ));
    }

    let mut cursor = &vectors_data[..];
    let vec_count = read_u32(&mut cursor)
        .map_err(|e| VectorError::Internal(format!("read vec count: {}", e)))?
        as usize;

    let mut vectors: Vec<(usize, Vec<f32>)> = Vec::with_capacity(vec_count);
    for _ in 0..vec_count {
        let internal_id = read_u32(&mut cursor)
            .map_err(|e| VectorError::Internal(format!("read internal_id: {}", e)))?
            as usize;
        let mut vec = Vec::with_capacity(dimension);
        for _ in 0..dimension {
            let mut fbuf = [0u8; 4];
            cursor
                .read_exact(&mut fbuf)
                .map_err(|e| VectorError::Internal(format!("read float: {}", e)))?;
            vec.push(f32::from_le_bytes(fbuf));
        }
        vectors.push((internal_id, vec));
    }

    // Read graph section
    let (tag, graph_data) = read_section(&mut r)?;
    if tag != SECTION_GRAPH {
        return Err(VectorError::Internal("expected graph section".to_string()));
    }

    let mut cursor = &graph_data[..];
    let num_layers = read_u32(&mut cursor)
        .map_err(|e| VectorError::Internal(format!("read num_layers: {}", e)))?
        as usize;

    let mut layers: Vec<HashMap<usize, Vec<usize>>> = Vec::with_capacity(num_layers);
    for _ in 0..num_layers {
        let num_nodes = read_u32(&mut cursor)
            .map_err(|e| VectorError::Internal(format!("read num_nodes: {}", e)))?
            as usize;
        let mut layer = HashMap::with_capacity(num_nodes);
        for _ in 0..num_nodes {
            let node = read_u32(&mut cursor)
                .map_err(|e| VectorError::Internal(format!("read node: {}", e)))?
                as usize;
            let num_neighbors = read_u32(&mut cursor)
                .map_err(|e| VectorError::Internal(format!("read num_neighbors: {}", e)))?
                as usize;
            let mut neighbors = Vec::with_capacity(num_neighbors);
            for _ in 0..num_neighbors {
                let n = read_u32(&mut cursor)
                    .map_err(|e| VectorError::Internal(format!("read neighbor: {}", e)))?
                    as usize;
                neighbors.push(n);
            }
            layer.insert(node, neighbors);
        }
        layers.push(layer);
    }

    // Read mappings section
    let (tag, mappings_data) = read_section(&mut r)?;
    if tag != SECTION_MAPPINGS {
        return Err(VectorError::Internal(
            "expected mappings section".to_string(),
        ));
    }

    let mut cursor = &mappings_data[..];
    let map_count = read_u32(&mut cursor)
        .map_err(|e| VectorError::Internal(format!("read map count: {}", e)))?
        as usize;

    let mut id_to_internal: HashMap<VectorId, usize> = HashMap::with_capacity(map_count);
    let mut internal_to_id: HashMap<usize, VectorId> = HashMap::with_capacity(map_count);
    let mut node_levels: HashMap<usize, usize> = HashMap::with_capacity(map_count);

    for _ in 0..map_count {
        let internal_id = read_u32(&mut cursor)
            .map_err(|e| VectorError::Internal(format!("read mapping internal_id: {}", e)))?
            as usize;
        let level = read_u32(&mut cursor)
            .map_err(|e| VectorError::Internal(format!("read node level: {}", e)))?
            as usize;
        let id_len = read_u32(&mut cursor)
            .map_err(|e| VectorError::Internal(format!("read id_len: {}", e)))?
            as usize;
        let mut id_bytes = vec![0u8; id_len];
        cursor
            .read_exact(&mut id_bytes)
            .map_err(|e| VectorError::Internal(format!("read id bytes: {}", e)))?;
        let external_id = VectorId::new(
            String::from_utf8(id_bytes)
                .map_err(|e| VectorError::Internal(format!("invalid utf8 id: {}", e)))?,
        );

        id_to_internal.insert(external_id.clone(), internal_id);
        internal_to_id.insert(internal_id, external_id);
        node_levels.insert(internal_id, level);
    }

    let entry_point = stored_entry_point;

    // Reconstruct the HNSW index
    let index = HnswIndex::from_raw_parts(
        dimension,
        metric,
        m,
        ef_construction,
        50, // default ef_search
        vectors,
        id_to_internal,
        internal_to_id,
        layers,
        entry_point,
        max_level,
        node_levels,
        element_count as usize,
    );

    info!(
        "deserialized HNSW index: {} vectors, {} dimensions",
        element_count, dimension
    );
    Ok(index)
}

/// Attempt to recover a corrupt index by rebuilding from source vectors.
///
/// If the index file is partially readable, extracts vectors and mappings,
/// then rebuilds the HNSW graph from scratch.
pub fn recover_hnsw<R: Read>(
    mut r: R,
    dimension: usize,
    metric: DistanceMetric,
    m: usize,
    ef_construction: usize,
) -> Result<HnswIndex, VectorError> {
    warn!("attempting HNSW index recovery — rebuilding from source vectors");

    // Try to read magic
    let mut magic_buf = [0u8; 8];
    if r.read_exact(&mut magic_buf).is_err() || &magic_buf != MAGIC {
        return Err(VectorError::Internal(
            "cannot recover: invalid magic bytes".to_string(),
        ));
    }

    // Try to read header (skip if corrupt)
    let header_result = read_section(&mut r);
    let (dim, met, m_val, ef_c) = if let Ok((SECTION_HEADER, data)) = header_result {
        let mut c = &data[..];
        let _version = read_u32(&mut c).unwrap_or(FORMAT_VERSION);
        let dim = read_u32(&mut c).unwrap_or(dimension as u32) as usize;
        let mut mbuf = [0u8; 1];
        let met = if c.read_exact(&mut mbuf).is_ok() {
            u8_to_metric(mbuf[0]).unwrap_or(metric)
        } else {
            metric
        };
        let _count = read_u64(&mut c).unwrap_or(0);
        let m_val = read_u32(&mut c).unwrap_or(m as u32) as usize;
        let ef_c = read_u32(&mut c).unwrap_or(ef_construction as u32) as usize;
        (dim, met, m_val, ef_c)
    } else {
        (dimension, metric, m, ef_construction)
    };

    // Try to read vectors
    let vectors_result = read_section(&mut r);
    let mut recovered_vectors: Vec<(String, Vec<f32>)> = Vec::new();

    if let Ok((SECTION_VECTORS, data)) = vectors_result {
        let mut cursor = &data[..];
        if let Ok(count) = read_u32(&mut cursor) {
            for _ in 0..count {
                let internal_id = match read_u32(&mut cursor) {
                    Ok(id) => id,
                    Err(_) => break,
                };
                let mut vec = Vec::with_capacity(dim);
                let mut ok = true;
                for _ in 0..dim {
                    let mut fbuf = [0u8; 4];
                    if cursor.read_exact(&mut fbuf).is_err() {
                        ok = false;
                        break;
                    }
                    vec.push(f32::from_le_bytes(fbuf));
                }
                if ok {
                    recovered_vectors.push((format!("recovered_{}", internal_id), vec));
                }
            }
        }
    }

    // Try to read mappings to get original IDs
    let mappings_result = {
        // Skip graph section if present
        let _ = read_section(&mut r);
        read_section(&mut r)
    };

    let mut id_map: HashMap<usize, String> = HashMap::new();
    if let Ok((SECTION_MAPPINGS, data)) = mappings_result {
        let mut cursor = &data[..];
        if let Ok(count) = read_u32(&mut cursor) {
            for _ in 0..count {
                let internal_id = match read_u32(&mut cursor) {
                    Ok(id) => id as usize,
                    Err(_) => break,
                };
                let id_len = match read_u32(&mut cursor) {
                    Ok(l) => l as usize,
                    Err(_) => break,
                };
                let mut id_bytes = vec![0u8; id_len];
                if cursor.read_exact(&mut id_bytes).is_err() {
                    break;
                }
                if let Ok(s) = String::from_utf8(id_bytes) {
                    id_map.insert(internal_id, s);
                }
            }
        }
    }

    // Rebuild index
    let index = HnswIndex::with_params(dim, met, m_val, ef_c, 50);

    for (default_id, vec) in &recovered_vectors {
        // Try to find original ID from mapping
        let internal_num: usize = default_id
            .strip_prefix("recovered_")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let id = id_map
            .get(&internal_num)
            .cloned()
            .unwrap_or_else(|| default_id.clone());

        if let Err(e) = index.add(VectorId::new(id.clone()), vec) {
            warn!("skipping vector {} during recovery: {}", id, e);
        }
    }

    info!(
        "recovered HNSW index: {} of {} vectors rebuilt",
        index.len(),
        recovered_vectors.len()
    );
    Ok(index)
}

/// Track which nodes have changed for incremental persistence
#[derive(Debug, Default)]
pub struct ChangeTracker {
    /// Node IDs that have been modified (added/updated/removed)
    changed_nodes: std::collections::HashSet<usize>,
    /// Whether graph structure changed
    graph_dirty: bool,
}

impl ChangeTracker {
    /// Create a new change tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark a node as changed
    pub fn mark_changed(&mut self, node_id: usize) {
        self.changed_nodes.insert(node_id);
        self.graph_dirty = true;
    }

    /// Check if there are pending changes
    pub fn has_changes(&self) -> bool {
        !self.changed_nodes.is_empty() || self.graph_dirty
    }

    /// Get changed node IDs
    pub fn changed_nodes(&self) -> &std::collections::HashSet<usize> {
        &self.changed_nodes
    }

    /// Reset tracker after persistence
    pub fn reset(&mut self) {
        self.changed_nodes.clear();
        self.graph_dirty = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_index() -> HnswIndex {
        let index = HnswIndex::with_params(3, DistanceMetric::Cosine, 4, 50, 20);
        index.add(VectorId::new("v1"), &[1.0, 0.0, 0.0]).unwrap();
        index.add(VectorId::new("v2"), &[0.0, 1.0, 0.0]).unwrap();
        index.add(VectorId::new("v3"), &[0.0, 0.0, 1.0]).unwrap();
        index.add(VectorId::new("v4"), &[0.7, 0.7, 0.0]).unwrap();
        index
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let original = make_test_index();
        let mut buf = Vec::new();
        serialize_hnsw(&original, &mut buf).unwrap();

        let restored = deserialize_hnsw(&buf[..]).unwrap();

        assert_eq!(restored.dimension(), original.dimension());
        assert_eq!(restored.metric(), original.metric());
        assert_eq!(restored.len(), original.len());

        // Search should produce same results
        let query = [1.0, 0.0, 0.0];
        let orig_results = original.search(&query, 4).unwrap();
        let rest_results = restored.search(&query, 4).unwrap();
        assert_eq!(orig_results.len(), rest_results.len());
        for (o, r) in orig_results.iter().zip(rest_results.iter()) {
            assert_eq!(o.id.as_str(), r.id.as_str());
        }
    }

    #[test]
    fn test_serialize_empty_index() {
        let index = HnswIndex::new(8, DistanceMetric::Euclidean);
        let mut buf = Vec::new();
        serialize_hnsw(&index, &mut buf).unwrap();

        let restored = deserialize_hnsw(&buf[..]).unwrap();
        assert_eq!(restored.dimension(), 8);
        assert_eq!(restored.len(), 0);
        assert_eq!(restored.metric(), DistanceMetric::Euclidean);
    }

    #[test]
    fn test_invalid_magic() {
        let data = b"NOTMAGIC rest of data here";
        let result = deserialize_hnsw(&data[..]);
        assert!(result.is_err());
    }

    #[test]
    fn test_crc32_corruption_detected() {
        let index = make_test_index();
        let mut buf = Vec::new();
        serialize_hnsw(&index, &mut buf).unwrap();

        // Corrupt a byte in the vectors section
        if buf.len() > 50 {
            buf[50] ^= 0xFF;
        }

        let result = deserialize_hnsw(&buf[..]);
        assert!(result.is_err());
    }

    #[test]
    fn test_recover_from_source_vectors() {
        let original = make_test_index();
        let mut buf = Vec::new();
        serialize_hnsw(&original, &mut buf).unwrap();

        // Recovery with correct params
        let recovered = recover_hnsw(&buf[..], 3, DistanceMetric::Cosine, 4, 50).unwrap();
        assert_eq!(recovered.len(), original.len());

        // Should be searchable
        let results = recovered.search(&[1.0, 0.0, 0.0], 2).unwrap();
        assert!(!results.is_empty());
    }

    #[test]
    fn test_change_tracker() {
        let mut tracker = ChangeTracker::new();
        assert!(!tracker.has_changes());

        tracker.mark_changed(0);
        tracker.mark_changed(5);
        assert!(tracker.has_changes());
        assert_eq!(tracker.changed_nodes().len(), 2);

        tracker.reset();
        assert!(!tracker.has_changes());
    }

    #[test]
    fn test_metric_roundtrip() {
        for metric in &[
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
            DistanceMetric::Manhattan,
        ] {
            let v = metric_to_u8(*metric);
            let back = u8_to_metric(v).unwrap();
            assert_eq!(*metric, back);
        }
    }

    #[test]
    fn test_large_index_persistence() {
        let index = HnswIndex::with_params(32, DistanceMetric::Euclidean, 8, 50, 20);
        for i in 0..200 {
            let vec: Vec<f32> = (0..32).map(|j| ((i * 7 + j) as f32) / 100.0).collect();
            index
                .add(VectorId::new(format!("vec_{}", i)), &vec)
                .unwrap();
        }

        let mut buf = Vec::new();
        serialize_hnsw(&index, &mut buf).unwrap();

        let restored = deserialize_hnsw(&buf[..]).unwrap();
        assert_eq!(restored.len(), 200);
        assert_eq!(restored.dimension(), 32);

        let query: Vec<f32> = (0..32).map(|j| (j as f32) / 100.0).collect();
        let results = restored.search(&query, 5).unwrap();
        assert_eq!(results.len(), 5);
    }
}
