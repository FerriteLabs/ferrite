//! Geo command implementations on CommandExecutor (GEOADD, GEOPOS, GEODIST, GEOSEARCH, etc.).

use bytes::Bytes;

use crate::commands::geo;
use crate::protocol::Frame;

use super::CommandExecutor;

impl CommandExecutor {
    // Geo commands

    pub(super) fn geoadd(
        &self,
        db: u8,
        key: &Bytes,
        items: Vec<(f64, f64, Bytes)>,
        nx: bool,
        xx: bool,
        ch: bool,
    ) -> Frame {
        geo::geoadd(&self.store, db, key, items, nx, xx, ch)
    }

    pub(super) fn geopos(&self, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
        geo::geopos(&self.store, db, key, members)
    }

    pub(super) fn geodist(
        &self,
        db: u8,
        key: &Bytes,
        member1: &Bytes,
        member2: &Bytes,
        unit: &str,
    ) -> Frame {
        let unit = geo::DistanceUnit::parse(unit).unwrap_or(geo::DistanceUnit::Meters);
        geo::geodist(&self.store, db, key, member1, member2, unit)
    }

    pub(super) fn geohash(&self, db: u8, key: &Bytes, members: &[Bytes]) -> Frame {
        geo::geohash(&self.store, db, key, members)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn georadius(
        &self,
        db: u8,
        key: &Bytes,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: &str,
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
        count: Option<usize>,
        asc: bool,
    ) -> Frame {
        let unit = geo::DistanceUnit::parse(unit).unwrap_or(geo::DistanceUnit::Meters);
        geo::georadius(
            &self.store,
            db,
            key,
            longitude,
            latitude,
            radius,
            unit,
            with_coord,
            with_dist,
            with_hash,
            count,
            asc,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn georadiusbymember(
        &self,
        db: u8,
        key: &Bytes,
        member: &Bytes,
        radius: f64,
        unit: &str,
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
        count: Option<usize>,
        asc: bool,
    ) -> Frame {
        let unit = geo::DistanceUnit::parse(unit).unwrap_or(geo::DistanceUnit::Meters);
        geo::georadiusbymember(
            &self.store,
            db,
            key,
            member,
            radius,
            unit,
            with_coord,
            with_dist,
            with_hash,
            count,
            asc,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn geosearch(
        &self,
        db: u8,
        key: &Bytes,
        from_member: Option<Bytes>,
        from_lonlat: Option<(f64, f64)>,
        by_radius: Option<(f64, String)>,
        by_box: Option<(f64, f64, String)>,
        asc: bool,
        count: Option<usize>,
        any: bool,
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
    ) -> Frame {
        // Build center
        let center = if let Some(member) = from_member {
            geo::GeoSearchCenter::Member(member)
        } else if let Some((lon, lat)) = from_lonlat {
            geo::GeoSearchCenter::LonLat(lon, lat)
        } else {
            return Frame::error("ERR FROMMEMBER or FROMLONLAT is required for GEOSEARCH");
        };

        // Build shape
        let shape = if let Some((radius, unit_str)) = by_radius {
            let unit = geo::DistanceUnit::parse(&unit_str).unwrap_or(geo::DistanceUnit::Meters);
            geo::GeoSearchShape::Radius(radius, unit)
        } else if let Some((width, height, unit_str)) = by_box {
            let unit = geo::DistanceUnit::parse(&unit_str).unwrap_or(geo::DistanceUnit::Meters);
            geo::GeoSearchShape::Box(width, height, unit)
        } else {
            return Frame::error("ERR BYRADIUS or BYBOX is required for GEOSEARCH");
        };

        let options = geo::GeoSearchOptions {
            center,
            shape,
            count,
            any,
            asc,
            with_coord,
            with_dist,
            with_hash,
        };

        geo::geosearch(&self.store, db, key, &options)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn geosearchstore(
        &self,
        db: u8,
        destination: &Bytes,
        source: &Bytes,
        from_member: Option<Bytes>,
        from_lonlat: Option<(f64, f64)>,
        by_radius: Option<(f64, String)>,
        by_box: Option<(f64, f64, String)>,
        asc: bool,
        count: Option<usize>,
        any: bool,
        storedist: bool,
    ) -> Frame {
        // Build center
        let center = if let Some(member) = from_member {
            geo::GeoSearchCenter::Member(member)
        } else if let Some((lon, lat)) = from_lonlat {
            geo::GeoSearchCenter::LonLat(lon, lat)
        } else {
            return Frame::error("ERR FROMMEMBER or FROMLONLAT is required for GEOSEARCHSTORE");
        };

        // Build shape
        let shape = if let Some((radius, unit_str)) = by_radius {
            let unit = geo::DistanceUnit::parse(&unit_str).unwrap_or(geo::DistanceUnit::Meters);
            geo::GeoSearchShape::Radius(radius, unit)
        } else if let Some((width, height, unit_str)) = by_box {
            let unit = geo::DistanceUnit::parse(&unit_str).unwrap_or(geo::DistanceUnit::Meters);
            geo::GeoSearchShape::Box(width, height, unit)
        } else {
            return Frame::error("ERR BYRADIUS or BYBOX is required for GEOSEARCHSTORE");
        };

        let options = geo::GeoSearchOptions {
            center,
            shape,
            count,
            any,
            asc,
            with_coord: false,
            with_dist: false,
            with_hash: false,
        };

        geo::geosearchstore(&self.store, db, destination, source, &options, storedist)
    }
}
