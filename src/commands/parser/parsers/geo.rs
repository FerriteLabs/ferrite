use bytes::Bytes;

use super::{get_bytes, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) fn parse_geoadd(args: &[Frame]) -> Result<Command> {
    if args.len() < 4 {
        return Err(FerriteError::WrongArity("GEOADD".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut idx = 1;
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;

    // Parse options
    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "NX" => {
                nx = true;
                idx += 1;
            }
            "XX" => {
                xx = true;
                idx += 1;
            }
            "CH" => {
                ch = true;
                idx += 1;
            }
            _ => break,
        }
    }

    // Parse longitude latitude member triplets
    let remaining = args.len() - idx;
    if remaining < 3 || remaining % 3 != 0 {
        return Err(FerriteError::Protocol(
            "Invalid number of arguments for GEOADD".to_string(),
        ));
    }

    let mut items = Vec::new();
    while idx + 2 < args.len() {
        let lon: f64 = get_string(&args[idx])?
            .parse()
            .map_err(|_| FerriteError::Protocol("Invalid longitude".to_string()))?;
        let lat: f64 = get_string(&args[idx + 1])?
            .parse()
            .map_err(|_| FerriteError::Protocol("Invalid latitude".to_string()))?;
        let member = get_bytes(&args[idx + 2])?;
        items.push((lon, lat, member));
        idx += 3;
    }

    Ok(Command::GeoAdd {
        key,
        items,
        nx,
        xx,
        ch,
    })
}

pub(crate) fn parse_geopos(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("GEOPOS".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let members: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();

    Ok(Command::GeoPos {
        key,
        members: members?,
    })
}

pub(crate) fn parse_geodist(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("GEODIST".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let member1 = get_bytes(&args[1])?;
    let member2 = get_bytes(&args[2])?;
    let unit = if args.len() > 3 {
        get_string(&args[3])?.to_lowercase()
    } else {
        "m".to_string()
    };

    Ok(Command::GeoDist {
        key,
        member1,
        member2,
        unit,
    })
}

pub(crate) fn parse_geohash(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("GEOHASH".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let members: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();

    Ok(Command::GeoHash {
        key,
        members: members?,
    })
}

pub(crate) fn parse_georadius(args: &[Frame]) -> Result<Command> {
    if args.len() < 5 {
        return Err(FerriteError::WrongArity("GEORADIUS".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let longitude: f64 = get_string(&args[1])?
        .parse()
        .map_err(|_| FerriteError::Protocol("Invalid longitude".to_string()))?;
    let latitude: f64 = get_string(&args[2])?
        .parse()
        .map_err(|_| FerriteError::Protocol("Invalid latitude".to_string()))?;
    let radius: f64 = get_string(&args[3])?
        .parse()
        .map_err(|_| FerriteError::Protocol("Invalid radius".to_string()))?;
    let unit = get_string(&args[4])?.to_lowercase();

    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut count = None;
    let mut asc = true;

    let mut idx = 5;
    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "WITHCOORD" => with_coord = true,
            "WITHDIST" => with_dist = true,
            "WITHHASH" => with_hash = true,
            "ASC" => asc = true,
            "DESC" => asc = false,
            "COUNT" => {
                idx += 1;
                if idx < args.len() {
                    count = Some(get_int(&args[idx])? as usize);
                }
            }
            _ => {}
        }
        idx += 1;
    }

    Ok(Command::GeoRadius {
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
    })
}

pub(crate) fn parse_georadiusbymember(args: &[Frame]) -> Result<Command> {
    if args.len() < 4 {
        return Err(FerriteError::WrongArity("GEORADIUSBYMEMBER".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let member = get_bytes(&args[1])?;
    let radius: f64 = get_string(&args[2])?
        .parse()
        .map_err(|_| FerriteError::Protocol("Invalid radius".to_string()))?;
    let unit = get_string(&args[3])?.to_lowercase();

    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut count = None;
    let mut asc = true;

    let mut idx = 4;
    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "WITHCOORD" => with_coord = true,
            "WITHDIST" => with_dist = true,
            "WITHHASH" => with_hash = true,
            "ASC" => asc = true,
            "DESC" => asc = false,
            "COUNT" => {
                idx += 1;
                if idx < args.len() {
                    count = Some(get_int(&args[idx])? as usize);
                }
            }
            _ => {}
        }
        idx += 1;
    }

    Ok(Command::GeoRadiusByMember {
        key,
        member,
        radius,
        unit,
        with_coord,
        with_dist,
        with_hash,
        count,
        asc,
    })
}

pub(crate) fn parse_geosearch(args: &[Frame]) -> Result<Command> {
    if args.len() < 4 {
        return Err(FerriteError::WrongArity("GEOSEARCH".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut from_member = None;
    let mut from_lonlat = None;
    let mut by_radius = None;
    let mut by_box = None;
    let mut asc = true;
    let mut count = None;
    let mut any = false;
    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;

    let mut idx = 1;
    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "FROMMEMBER" => {
                idx += 1;
                if idx < args.len() {
                    from_member = Some(get_bytes(&args[idx])?);
                }
            }
            "FROMLONLAT" => {
                if idx + 2 < args.len() {
                    let lon: f64 = get_string(&args[idx + 1])?
                        .parse()
                        .map_err(|_| FerriteError::Protocol("Invalid longitude".to_string()))?;
                    let lat: f64 = get_string(&args[idx + 2])?
                        .parse()
                        .map_err(|_| FerriteError::Protocol("Invalid latitude".to_string()))?;
                    from_lonlat = Some((lon, lat));
                    idx += 2;
                }
            }
            "BYRADIUS" => {
                if idx + 2 < args.len() {
                    let r: f64 = get_string(&args[idx + 1])?
                        .parse()
                        .map_err(|_| FerriteError::Protocol("Invalid radius".to_string()))?;
                    let u = get_string(&args[idx + 2])?.to_lowercase();
                    by_radius = Some((r, u));
                    idx += 2;
                }
            }
            "BYBOX" => {
                if idx + 3 < args.len() {
                    let w: f64 = get_string(&args[idx + 1])?
                        .parse()
                        .map_err(|_| FerriteError::Protocol("Invalid width".to_string()))?;
                    let h: f64 = get_string(&args[idx + 2])?
                        .parse()
                        .map_err(|_| FerriteError::Protocol("Invalid height".to_string()))?;
                    let u = get_string(&args[idx + 3])?.to_lowercase();
                    by_box = Some((w, h, u));
                    idx += 3;
                }
            }
            "ASC" => asc = true,
            "DESC" => asc = false,
            "COUNT" => {
                idx += 1;
                if idx < args.len() {
                    count = Some(get_int(&args[idx])? as usize);
                }
            }
            "ANY" => any = true,
            "WITHCOORD" => with_coord = true,
            "WITHDIST" => with_dist = true,
            "WITHHASH" => with_hash = true,
            _ => {}
        }
        idx += 1;
    }

    Ok(Command::GeoSearch {
        key,
        from_member,
        from_lonlat,
        by_radius,
        by_box,
        asc,
        count,
        any,
        with_coord,
        with_dist,
        with_hash,
    })
}

pub(crate) fn parse_geosearchstore(args: &[Frame]) -> Result<Command> {
    if args.len() < 6 {
        return Err(FerriteError::WrongArity("GEOSEARCHSTORE".to_string()));
    }

    let destination = get_bytes(&args[0])?;
    let source = get_bytes(&args[1])?;
    let mut from_member = None;
    let mut from_lonlat = None;
    let mut by_radius = None;
    let mut by_box = None;
    let mut asc = true;
    let mut count = None;
    let mut any = false;
    let mut storedist = false;

    let mut idx = 2;
    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "FROMMEMBER" => {
                idx += 1;
                if idx < args.len() {
                    from_member = Some(get_bytes(&args[idx])?);
                }
            }
            "FROMLONLAT" => {
                if idx + 2 < args.len() {
                    let lon: f64 = get_string(&args[idx + 1])?
                        .parse()
                        .map_err(|_| FerriteError::Protocol("Invalid longitude".to_string()))?;
                    let lat: f64 = get_string(&args[idx + 2])?
                        .parse()
                        .map_err(|_| FerriteError::Protocol("Invalid latitude".to_string()))?;
                    from_lonlat = Some((lon, lat));
                    idx += 2;
                }
            }
            "BYRADIUS" => {
                if idx + 2 < args.len() {
                    let r: f64 = get_string(&args[idx + 1])?
                        .parse()
                        .map_err(|_| FerriteError::Protocol("Invalid radius".to_string()))?;
                    let u = get_string(&args[idx + 2])?.to_lowercase();
                    by_radius = Some((r, u));
                    idx += 2;
                }
            }
            "BYBOX" => {
                if idx + 3 < args.len() {
                    let w: f64 = get_string(&args[idx + 1])?
                        .parse()
                        .map_err(|_| FerriteError::Protocol("Invalid width".to_string()))?;
                    let h: f64 = get_string(&args[idx + 2])?
                        .parse()
                        .map_err(|_| FerriteError::Protocol("Invalid height".to_string()))?;
                    let u = get_string(&args[idx + 3])?.to_lowercase();
                    by_box = Some((w, h, u));
                    idx += 3;
                }
            }
            "ASC" => asc = true,
            "DESC" => asc = false,
            "COUNT" => {
                idx += 1;
                if idx < args.len() {
                    count = Some(get_int(&args[idx])? as usize);
                }
            }
            "ANY" => any = true,
            "STOREDIST" => storedist = true,
            _ => {}
        }
        idx += 1;
    }

    Ok(Command::GeoSearchStore {
        destination,
        source,
        from_member,
        from_lonlat,
        by_radius,
        by_box,
        asc,
        count,
        any,
        storedist,
    })
}
