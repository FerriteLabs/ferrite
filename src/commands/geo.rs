//! Redis Geo commands
//!
//! This module implements Redis Geo commands (GEOADD, GEOPOS, GEODIST, GEORADIUS, GEOSEARCH, etc.).
//! Geo data is stored in sorted sets with geohash encoded scores.

use std::f64::consts::PI;
use std::sync::Arc;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::{Store, Value};

/// Earth's radius in meters
const EARTH_RADIUS_METERS: f64 = 6_372_797.560_856;

/// Bits used for geohash encoding
const GEO_STEP: u32 = 26;

/// Geospatial coordinates
#[derive(Debug, Clone, Copy)]
pub struct GeoCoord {
    pub longitude: f64,
    pub latitude: f64,
}

impl GeoCoord {
    pub fn new(longitude: f64, latitude: f64) -> Option<Self> {
        // Validate coordinates
        if !(-180.0..=180.0).contains(&longitude) {
            return None;
        }
        if !(-85.051_128_78..=85.051_128_78).contains(&latitude) {
            return None;
        }
        Some(Self {
            longitude,
            latitude,
        })
    }
}

/// Distance unit for geo calculations
#[derive(Debug, Clone, Copy)]
pub enum DistanceUnit {
    Meters,
    Kilometers,
    Miles,
    Feet,
}

impl DistanceUnit {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "m" => Some(Self::Meters),
            "km" => Some(Self::Kilometers),
            "mi" => Some(Self::Miles),
            "ft" => Some(Self::Feet),
            _ => None,
        }
    }

    pub fn to_meters(self, value: f64) -> f64 {
        match self {
            Self::Meters => value,
            Self::Kilometers => value * 1_000.0,
            Self::Miles => value * 1609.34,
            Self::Feet => value * 0.3048,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn from_meters(self, value: f64) -> f64 {
        match self {
            Self::Meters => value,
            Self::Kilometers => value / 1_000.0,
            Self::Miles => value / 1609.34,
            Self::Feet => value / 0.3048,
        }
    }
}

/// Encode longitude/latitude to geohash score
pub fn geohash_encode(coord: &GeoCoord) -> f64 {
    let mut lat_range = (-85.051_128_78, 85.051_128_78);
    let mut lon_range = (-180.0, 180.0);

    let mut hash: u64 = 0;
    let mut bit = 0;

    while bit < GEO_STEP * 2 {
        // Longitude bit
        let mid = (lon_range.0 + lon_range.1) / 2.0;
        if coord.longitude >= mid {
            hash |= 1 << (63 - bit);
            lon_range.0 = mid;
        } else {
            lon_range.1 = mid;
        }
        bit += 1;

        // Latitude bit
        let mid = (lat_range.0 + lat_range.1) / 2.0;
        if coord.latitude >= mid {
            hash |= 1 << (63 - bit);
            lat_range.0 = mid;
        } else {
            lat_range.1 = mid;
        }
        bit += 1;
    }

    // Shift to get 52-bit integer (fits in f64 mantissa)
    (hash >> 12) as f64
}

/// Decode geohash score to longitude/latitude
pub fn geohash_decode(hash: f64) -> GeoCoord {
    let hash = (hash as u64) << 12;

    let mut lat_range = (-85.051_128_78, 85.051_128_78);
    let mut lon_range = (-180.0, 180.0);

    for bit in 0..(GEO_STEP * 2) {
        if bit % 2 == 0 {
            // Longitude bit
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if (hash >> (63 - bit)) & 1 == 1 {
                lon_range.0 = mid;
            } else {
                lon_range.1 = mid;
            }
        } else {
            // Latitude bit
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if (hash >> (63 - bit)) & 1 == 1 {
                lat_range.0 = mid;
            } else {
                lat_range.1 = mid;
            }
        }
    }

    GeoCoord {
        longitude: (lon_range.0 + lon_range.1) / 2.0,
        latitude: (lat_range.0 + lat_range.1) / 2.0,
    }
}

/// Convert geohash to base32 string representation
pub fn geohash_to_string(hash: f64) -> String {
    const ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let hash = hash as u64;

    // Use 11 characters for the string representation
    let mut result = String::with_capacity(11);
    let mut h = hash;

    for _ in 0..11 {
        let idx = ((h >> 47) & 0x1F) as usize;
        result.push(ALPHABET[idx] as char);
        h <<= 5;
    }

    result
}

/// Calculate distance between two coordinates using Haversine formula
pub fn haversine_distance(c1: &GeoCoord, c2: &GeoCoord) -> f64 {
    let lat1 = c1.latitude * PI / 180.0;
    let lat2 = c2.latitude * PI / 180.0;
    let lon1 = c1.longitude * PI / 180.0;
    let lon2 = c2.longitude * PI / 180.0;

    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;

    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_METERS * c
}

/// Execute GEOADD command
pub fn geoadd(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    items: Vec<(f64, f64, Bytes)>, // (longitude, latitude, member)
    nx: bool,
    xx: bool,
    ch: bool,
) -> Frame {
    use ordered_float::OrderedFloat;
    use std::collections::{BTreeMap, HashMap};

    // Get or create sorted set
    let (mut by_score, mut by_member) = match store.get(db, key) {
        Some(Value::SortedSet {
            by_score,
            by_member,
        }) => (by_score, by_member),
        Some(_) => {
            return Frame::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )
        }
        None => (BTreeMap::new(), HashMap::new()),
    };

    let mut added = 0i64;
    let mut changed = 0i64;

    for (lon, lat, member) in items {
        // Validate coordinates
        let coord = match GeoCoord::new(lon, lat) {
            Some(c) => c,
            None => return Frame::error("ERR invalid longitude,latitude pair, out of valid range"),
        };

        let score = geohash_encode(&coord);
        let exists = by_member.contains_key(&member);

        // Apply NX/XX options
        if nx && exists {
            continue;
        }
        if xx && !exists {
            continue;
        }

        // Remove old entry if exists
        if let Some(old_score) = by_member.get(&member) {
            if (*old_score - score).abs() < f64::EPSILON {
                continue; // Same score, no change
            }
            by_score.remove(&(OrderedFloat(*old_score), member.clone()));
            changed += 1;
        } else {
            added += 1;
        }

        // Add new entry
        by_member.insert(member.clone(), score);
        by_score.insert((OrderedFloat(score), member), ());
    }

    store.set(
        db,
        key.clone(),
        Value::SortedSet {
            by_score,
            by_member,
        },
    );

    if ch {
        Frame::Integer(added + changed)
    } else {
        Frame::Integer(added)
    }
}

/// Execute GEOPOS command
pub fn geopos(store: &Arc<Store>, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_member, .. }) => {
            let results: Vec<Frame> = members
                .iter()
                .map(|member| match by_member.get(member) {
                    Some(score) => {
                        let coord = geohash_decode(*score);
                        Frame::array(vec![
                            Frame::bulk(Bytes::from(format!("{:.6}", coord.longitude))),
                            Frame::bulk(Bytes::from(format!("{:.6}", coord.latitude))),
                        ])
                    }
                    None => Frame::null(),
                })
                .collect();
            Frame::array(results)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => {
            // Return array of nulls for non-existent key
            let results: Vec<Frame> = members.iter().map(|_| Frame::null()).collect();
            Frame::array(results)
        }
    }
}

/// Execute GEODIST command
pub fn geodist(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    member1: &Bytes,
    member2: &Bytes,
    unit: DistanceUnit,
) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_member, .. }) => {
            match (by_member.get(member1), by_member.get(member2)) {
                (Some(score1), Some(score2)) => {
                    let coord1 = geohash_decode(*score1);
                    let coord2 = geohash_decode(*score2);
                    let distance = haversine_distance(&coord1, &coord2);
                    let distance = unit.from_meters(distance);
                    Frame::bulk(Bytes::from(format!("{:.4}", distance)))
                }
                _ => Frame::null(),
            }
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::null(),
    }
}

/// Execute GEOHASH command
pub fn geohash(store: &Arc<Store>, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_member, .. }) => {
            let results: Vec<Frame> = members
                .iter()
                .map(|member| match by_member.get(member) {
                    Some(score) => {
                        let hash_str = geohash_to_string(*score);
                        Frame::bulk(Bytes::from(hash_str))
                    }
                    None => Frame::null(),
                })
                .collect();
            Frame::array(results)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => {
            let results: Vec<Frame> = members.iter().map(|_| Frame::null()).collect();
            Frame::array(results)
        }
    }
}

/// Search options for GEOSEARCH
#[derive(Debug, Clone)]
pub struct GeoSearchOptions {
    pub center: GeoSearchCenter,
    pub shape: GeoSearchShape,
    pub count: Option<usize>,
    #[allow(dead_code)] // Planned for v0.2 — reserved for GEOSEARCH ANY option support
    pub any: bool,
    pub asc: bool,
    pub with_coord: bool,
    pub with_dist: bool,
    pub with_hash: bool,
}

/// Center point for geo search
#[derive(Debug, Clone)]
pub enum GeoSearchCenter {
    Member(Bytes),
    LonLat(f64, f64),
}

/// Search shape for geo search
#[derive(Debug, Clone)]
pub enum GeoSearchShape {
    Radius(f64, DistanceUnit),
    Box(f64, f64, DistanceUnit), // width, height
}

/// Execute GEOSEARCH command
pub fn geosearch(store: &Arc<Store>, db: u8, key: &Bytes, options: &GeoSearchOptions) -> Frame {
    match store.get(db, key) {
        Some(Value::SortedSet { by_member, .. }) => {
            // Get center coordinates
            let center = match &options.center {
                GeoSearchCenter::LonLat(lon, lat) => match GeoCoord::new(*lon, *lat) {
                    Some(c) => c,
                    None => return Frame::error("ERR invalid longitude,latitude pair"),
                },
                GeoSearchCenter::Member(member) => match by_member.get(member) {
                    Some(score) => geohash_decode(*score),
                    None => return Frame::array(vec![]),
                },
            };

            // Collect matching members with distances
            let mut matches: Vec<(Bytes, f64, GeoCoord, f64)> = Vec::new();

            for (member, score) in by_member.iter() {
                let coord = geohash_decode(*score);
                let distance = haversine_distance(&center, &coord);

                let in_range = match &options.shape {
                    GeoSearchShape::Radius(radius, unit) => distance <= unit.to_meters(*radius),
                    GeoSearchShape::Box(width, height, unit) => {
                        // Approximate box check using lat/lon deltas
                        let width_m = unit.to_meters(*width);
                        let height_m = unit.to_meters(*height);

                        // Convert to degrees (approximate)
                        let lat_rad = center.latitude * PI / 180.0;
                        let lon_per_meter = 1.0 / (111_320.0 * lat_rad.cos());
                        let lat_per_meter = 1.0 / 110_540.0;

                        let dlon = (coord.longitude - center.longitude).abs();
                        let dlat = (coord.latitude - center.latitude).abs();

                        dlon <= (width_m / 2.0) * lon_per_meter
                            && dlat <= (height_m / 2.0) * lat_per_meter
                    }
                };

                if in_range {
                    matches.push((member.clone(), distance, coord, *score));
                }
            }

            // Sort by distance (using total_cmp for deterministic NaN handling)
            if options.asc {
                matches.sort_by(|a, b| a.1.total_cmp(&b.1));
            } else {
                matches.sort_by(|a, b| b.1.total_cmp(&a.1));
            }

            // Apply count limit
            if let Some(count) = options.count {
                matches.truncate(count);
            }

            // Get distance unit for output
            let output_unit = match &options.shape {
                GeoSearchShape::Radius(_, unit) => *unit,
                GeoSearchShape::Box(_, _, unit) => *unit,
            };

            // Build result
            let results: Vec<Frame> = matches
                .into_iter()
                .map(|(member, distance, coord, hash)| {
                    if options.with_coord || options.with_dist || options.with_hash {
                        let mut parts = vec![Frame::bulk(member)];
                        if options.with_dist {
                            parts.push(Frame::bulk(Bytes::from(format!(
                                "{:.4}",
                                output_unit.from_meters(distance)
                            ))));
                        }
                        if options.with_hash {
                            parts.push(Frame::Integer(hash as i64));
                        }
                        if options.with_coord {
                            parts.push(Frame::array(vec![
                                Frame::bulk(Bytes::from(format!("{:.6}", coord.longitude))),
                                Frame::bulk(Bytes::from(format!("{:.6}", coord.latitude))),
                            ]));
                        }
                        Frame::array(parts)
                    } else {
                        Frame::bulk(member)
                    }
                })
                .collect();

            Frame::array(results)
        }
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::array(vec![]),
    }
}

/// Execute GEORADIUS command (deprecated but still used)
#[allow(clippy::too_many_arguments)]
pub fn georadius(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    longitude: f64,
    latitude: f64,
    radius: f64,
    unit: DistanceUnit,
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    asc: bool,
) -> Frame {
    let options = GeoSearchOptions {
        center: GeoSearchCenter::LonLat(longitude, latitude),
        shape: GeoSearchShape::Radius(radius, unit),
        count,
        any: false,
        asc,
        with_coord,
        with_dist,
        with_hash,
    };
    geosearch(store, db, key, &options)
}

/// Execute GEORADIUSBYMEMBER command
#[allow(clippy::too_many_arguments)]
pub fn georadiusbymember(
    store: &Arc<Store>,
    db: u8,
    key: &Bytes,
    member: &Bytes,
    radius: f64,
    unit: DistanceUnit,
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    asc: bool,
) -> Frame {
    let options = GeoSearchOptions {
        center: GeoSearchCenter::Member(member.clone()),
        shape: GeoSearchShape::Radius(radius, unit),
        count,
        any: false,
        asc,
        with_coord,
        with_dist,
        with_hash,
    };
    geosearch(store, db, key, &options)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn create_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[test]
    fn test_geohash_encode_decode() {
        let coord = GeoCoord::new(-122.4194, 37.7749).unwrap(); // San Francisco
        let hash = geohash_encode(&coord);
        let decoded = geohash_decode(hash);

        // Should be accurate to about 0.6m
        assert!((coord.longitude - decoded.longitude).abs() < 0.0001);
        assert!((coord.latitude - decoded.latitude).abs() < 0.0001);
    }

    #[test]
    fn test_haversine_distance() {
        let sf = GeoCoord::new(-122.4194, 37.7749).unwrap();
        let la = GeoCoord::new(-118.2437, 34.0522).unwrap();

        let distance = haversine_distance(&sf, &la);

        // SF to LA is about 559 km
        assert!(distance > 550_000.0 && distance < 570_000.0);
    }

    #[test]
    fn test_geoadd_and_geopos() {
        let store = create_store();
        let key = Bytes::from("locations");

        // Add San Francisco
        let result = geoadd(
            &store,
            0,
            &key,
            vec![(-122.4194, 37.7749, Bytes::from("sf"))],
            false,
            false,
            false,
        );
        assert!(matches!(result, Frame::Integer(1)));

        // Get position
        let result = geopos(&store, 0, &key, &[Bytes::from("sf")]);
        if let Frame::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 1);
            if let Frame::Array(Some(coords)) = &arr[0] {
                assert_eq!(coords.len(), 2);
            }
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_geodist() {
        let store = create_store();
        let key = Bytes::from("locations");

        geoadd(
            &store,
            0,
            &key,
            vec![
                (-122.4194, 37.7749, Bytes::from("sf")),
                (-118.2437, 34.0522, Bytes::from("la")),
            ],
            false,
            false,
            false,
        );

        let result = geodist(
            &store,
            0,
            &key,
            &Bytes::from("sf"),
            &Bytes::from("la"),
            DistanceUnit::Kilometers,
        );
        if let Frame::Bulk(Some(dist)) = result {
            let dist_str = String::from_utf8_lossy(&dist);
            let dist: f64 = dist_str.parse().unwrap();
            assert!(dist > 550.0 && dist < 570.0);
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_geosearch_radius() {
        let store = create_store();
        let key = Bytes::from("locations");

        geoadd(
            &store,
            0,
            &key,
            vec![
                (-122.4194, 37.7749, Bytes::from("sf")),
                (-122.2585, 37.8716, Bytes::from("berkeley")),
                (-118.2437, 34.0522, Bytes::from("la")),
            ],
            false,
            false,
            false,
        );

        // Search within 50km of SF
        let options = GeoSearchOptions {
            center: GeoSearchCenter::Member(Bytes::from("sf")),
            shape: GeoSearchShape::Radius(50.0, DistanceUnit::Kilometers),
            count: None,
            any: false,
            asc: true,
            with_coord: false,
            with_dist: false,
            with_hash: false,
        };

        let result = geosearch(&store, 0, &key, &options);
        if let Frame::Array(Some(arr)) = result {
            // Should find SF and Berkeley, but not LA
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected array");
        }
    }
}
