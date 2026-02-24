use bytes::Bytes;

use super::{get_bytes, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) fn parse_zadd(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("ZADD".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut options = crate::commands::sorted_sets::ZAddOptions::default();
    let mut idx = 1;

    // Parse options (NX, XX, GT, LT, CH)
    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_ascii_uppercase();
        match opt.as_str() {
            "NX" => {
                options.nx = true;
                idx += 1;
            }
            "XX" => {
                options.xx = true;
                idx += 1;
            }
            "GT" => {
                options.gt = true;
                idx += 1;
            }
            "LT" => {
                options.lt = true;
                idx += 1;
            }
            "CH" => {
                options.ch = true;
                idx += 1;
            }
            _ => break,
        }
    }

    // Remaining args should be score member pairs
    let remaining = &args[idx..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return Err(FerriteError::WrongArity("ZADD".to_string()));
    }

    let mut pairs = Vec::with_capacity(remaining.len() / 2);
    for chunk in remaining.chunks(2) {
        let score_str = get_string(&chunk[0])?;
        let score = parse_score(&score_str)?;
        let member = get_bytes(&chunk[1])?;
        pairs.push((score, member));
    }

    Ok(Command::ZAdd {
        key,
        options,
        pairs,
    })
}

pub(crate) fn parse_score(s: &str) -> Result<f64> {
    match s.to_lowercase().as_str() {
        "+inf" | "inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        _ => s.parse::<f64>().map_err(|_| FerriteError::NotFloat),
    }
}

pub(crate) fn parse_zrem(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("ZREM".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let members: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::ZRem {
        key,
        members: members?,
    })
}

pub(crate) fn parse_zscore(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("ZSCORE".to_string()));
    }
    Ok(Command::ZScore {
        key: get_bytes(&args[0])?,
        member: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_zcard(args: &[Frame]) -> Result<Command> {
    if args.len() != 1 {
        return Err(FerriteError::WrongArity("ZCARD".to_string()));
    }
    Ok(Command::ZCard {
        key: get_bytes(&args[0])?,
    })
}

pub(crate) fn parse_zcount(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("ZCOUNT".to_string()));
    }
    let min_str = get_string(&args[1])?;
    let max_str = get_string(&args[2])?;
    Ok(Command::ZCount {
        key: get_bytes(&args[0])?,
        min: parse_score(&min_str)?,
        max: parse_score(&max_str)?,
    })
}

pub(crate) fn parse_zrank(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("ZRANK".to_string()));
    }
    Ok(Command::ZRank {
        key: get_bytes(&args[0])?,
        member: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_zrevrank(args: &[Frame]) -> Result<Command> {
    if args.len() != 2 {
        return Err(FerriteError::WrongArity("ZREVRANK".to_string()));
    }
    Ok(Command::ZRevRank {
        key: get_bytes(&args[0])?,
        member: get_bytes(&args[1])?,
    })
}

pub(crate) fn parse_zrange(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 || args.len() > 4 {
        return Err(FerriteError::WrongArity("ZRANGE".to_string()));
    }
    let with_scores = if args.len() == 4 {
        let opt = get_string(&args[3])?.to_ascii_uppercase();
        opt == "WITHSCORES"
    } else {
        false
    };
    Ok(Command::ZRange {
        key: get_bytes(&args[0])?,
        start: get_int(&args[1])?,
        stop: get_int(&args[2])?,
        with_scores,
    })
}

pub(crate) fn parse_zrevrange(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 || args.len() > 4 {
        return Err(FerriteError::WrongArity("ZREVRANGE".to_string()));
    }
    let with_scores = if args.len() == 4 {
        let opt = get_string(&args[3])?.to_ascii_uppercase();
        opt == "WITHSCORES"
    } else {
        false
    };
    Ok(Command::ZRevRange {
        key: get_bytes(&args[0])?,
        start: get_int(&args[1])?,
        stop: get_int(&args[2])?,
        with_scores,
    })
}

pub(crate) fn parse_zrangebyscore(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("ZRANGEBYSCORE".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let min_str = get_string(&args[1])?;
    let max_str = get_string(&args[2])?;
    let min = parse_score(&min_str)?;
    let max = parse_score(&max_str)?;

    let mut with_scores = false;
    let mut offset = None;
    let mut count = None;
    let mut idx = 3;

    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_ascii_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                with_scores = true;
                idx += 1;
            }
            "LIMIT" => {
                if idx + 2 >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                offset = Some(get_int(&args[idx + 1])? as usize);
                count = Some(get_int(&args[idx + 2])? as usize);
                idx += 3;
            }
            _ => return Err(FerriteError::Syntax),
        }
    }

    Ok(Command::ZRangeByScore {
        key,
        min,
        max,
        with_scores,
        offset,
        count,
    })
}

pub(crate) fn parse_zrevrangebyscore(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("ZREVRANGEBYSCORE".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let max_str = get_string(&args[1])?;
    let min_str = get_string(&args[2])?;
    let max = parse_score(&max_str)?;
    let min = parse_score(&min_str)?;

    let mut with_scores = false;
    let mut offset = None;
    let mut count = None;
    let mut idx = 3;

    while idx < args.len() {
        let opt = get_string(&args[idx])?.to_ascii_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                with_scores = true;
                idx += 1;
            }
            "LIMIT" => {
                if idx + 2 >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                offset = Some(get_int(&args[idx + 1])? as usize);
                count = Some(get_int(&args[idx + 2])? as usize);
                idx += 3;
            }
            _ => return Err(FerriteError::Syntax),
        }
    }

    Ok(Command::ZRevRangeByScore {
        key,
        max,
        min,
        with_scores,
        offset,
        count,
    })
}

pub(crate) fn parse_zincrby(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("ZINCRBY".to_string()));
    }
    let increment_str = get_string(&args[1])?;
    Ok(Command::ZIncrBy {
        key: get_bytes(&args[0])?,
        increment: parse_score(&increment_str)?,
        member: get_bytes(&args[2])?,
    })
}

pub(crate) fn parse_zpopmin(args: &[Frame]) -> Result<Command> {
    if args.is_empty() || args.len() > 2 {
        return Err(FerriteError::WrongArity("ZPOPMIN".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let count = if args.len() == 2 {
        Some(get_int(&args[1])? as usize)
    } else {
        None
    };
    Ok(Command::ZPopMin { key, count })
}

pub(crate) fn parse_zpopmax(args: &[Frame]) -> Result<Command> {
    if args.is_empty() || args.len() > 2 {
        return Err(FerriteError::WrongArity("ZPOPMAX".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let count = if args.len() == 2 {
        Some(get_int(&args[1])? as usize)
    } else {
        None
    };
    Ok(Command::ZPopMax { key, count })
}

pub(crate) fn parse_zmscore(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("ZMSCORE".to_string()));
    }
    let key = get_bytes(&args[0])?;
    let members: Result<Vec<Bytes>> = args[1..].iter().map(get_bytes).collect();
    Ok(Command::ZMScore {
        key,
        members: members?,
    })
}

/// Helper to parse WEIGHTS, AGGREGATE, and WITHSCORES options for ZUNION/ZINTER
pub(crate) fn parse_zset_op_options(
    args: &[Frame],
    numkeys: usize,
) -> Result<(
    Option<Vec<f64>>,
    crate::commands::sorted_sets::Aggregate,
    bool,
)> {
    use crate::commands::sorted_sets::Aggregate;

    let mut weights = None;
    let mut aggregate = Aggregate::Sum;
    let mut withscores = false;
    let mut i = 1 + numkeys;

    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                i += 1;
                let mut w = Vec::with_capacity(numkeys);
                for _ in 0..numkeys {
                    if i >= args.len() {
                        return Err(FerriteError::Syntax);
                    }
                    let weight_str = get_string(&args[i])?;
                    let weight: f64 = weight_str.parse().map_err(|_| FerriteError::NotFloat)?;
                    w.push(weight);
                    i += 1;
                }
                weights = Some(w);
            }
            "AGGREGATE" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                let agg = get_string(&args[i])?.to_uppercase();
                aggregate = match agg.as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => return Err(FerriteError::Syntax),
                };
                i += 1;
            }
            "WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            _ => return Err(FerriteError::Syntax),
        }
    }

    Ok((weights, aggregate, withscores))
}

/// ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
pub(crate) fn parse_zunion(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("ZUNION".to_string()));
    }

    let numkeys_str = get_string(&args[0])?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| FerriteError::NotInteger)?;

    if args.len() < 1 + numkeys {
        return Err(FerriteError::WrongArity("ZUNION".to_string()));
    }

    let keys: Result<Vec<Bytes>> = args[1..=numkeys].iter().map(get_bytes).collect();
    let keys = keys?;

    let (weights, aggregate, withscores) = parse_zset_op_options(args, numkeys)?;

    Ok(Command::ZUnion {
        keys,
        weights,
        aggregate,
        withscores,
    })
}

/// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
pub(crate) fn parse_zunionstore(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("ZUNIONSTORE".to_string()));
    }

    let destination = get_bytes(&args[0])?;
    let numkeys_str = get_string(&args[1])?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| FerriteError::NotInteger)?;

    if args.len() < 2 + numkeys {
        return Err(FerriteError::WrongArity("ZUNIONSTORE".to_string()));
    }

    let keys: Result<Vec<Bytes>> = args[2..2 + numkeys].iter().map(get_bytes).collect();
    let keys = keys?;

    let (weights, aggregate, _) = parse_zset_op_options_store(&args[1..], numkeys)?;

    Ok(Command::ZUnionStore {
        destination,
        keys,
        weights,
        aggregate,
    })
}

/// Helper for store variants (args start from numkeys, not destination)
pub(crate) fn parse_zset_op_options_store(
    args: &[Frame],
    numkeys: usize,
) -> Result<(
    Option<Vec<f64>>,
    crate::commands::sorted_sets::Aggregate,
    bool,
)> {
    use crate::commands::sorted_sets::Aggregate;

    let mut weights = None;
    let mut aggregate = Aggregate::Sum;
    let mut i = 1 + numkeys;

    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                i += 1;
                let mut w = Vec::with_capacity(numkeys);
                for _ in 0..numkeys {
                    if i >= args.len() {
                        return Err(FerriteError::Syntax);
                    }
                    let weight_str = get_string(&args[i])?;
                    let weight: f64 = weight_str.parse().map_err(|_| FerriteError::NotFloat)?;
                    w.push(weight);
                    i += 1;
                }
                weights = Some(w);
            }
            "AGGREGATE" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                let agg = get_string(&args[i])?.to_uppercase();
                aggregate = match agg.as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => return Err(FerriteError::Syntax),
                };
                i += 1;
            }
            _ => return Err(FerriteError::Syntax),
        }
    }

    Ok((weights, aggregate, false))
}

/// ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
pub(crate) fn parse_zinter(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("ZINTER".to_string()));
    }

    let numkeys_str = get_string(&args[0])?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| FerriteError::NotInteger)?;

    if args.len() < 1 + numkeys {
        return Err(FerriteError::WrongArity("ZINTER".to_string()));
    }

    let keys: Result<Vec<Bytes>> = args[1..=numkeys].iter().map(get_bytes).collect();
    let keys = keys?;

    let (weights, aggregate, withscores) = parse_zset_op_options(args, numkeys)?;

    Ok(Command::ZInter {
        keys,
        weights,
        aggregate,
        withscores,
    })
}

/// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
pub(crate) fn parse_zinterstore(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("ZINTERSTORE".to_string()));
    }

    let destination = get_bytes(&args[0])?;
    let numkeys_str = get_string(&args[1])?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| FerriteError::NotInteger)?;

    if args.len() < 2 + numkeys {
        return Err(FerriteError::WrongArity("ZINTERSTORE".to_string()));
    }

    let keys: Result<Vec<Bytes>> = args[2..2 + numkeys].iter().map(get_bytes).collect();
    let keys = keys?;

    let (weights, aggregate, _) = parse_zset_op_options_store(&args[1..], numkeys)?;

    Ok(Command::ZInterStore {
        destination,
        keys,
        weights,
        aggregate,
    })
}

/// ZINTERCARD numkeys key [key ...] [LIMIT limit]
pub(crate) fn parse_zintercard(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("ZINTERCARD".to_string()));
    }

    let numkeys_str = get_string(&args[0])?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| FerriteError::NotInteger)?;

    if args.len() < 1 + numkeys {
        return Err(FerriteError::WrongArity("ZINTERCARD".to_string()));
    }

    let keys: Result<Vec<Bytes>> = args[1..=numkeys].iter().map(get_bytes).collect();
    let keys = keys?;

    let mut limit = None;
    let mut i = 1 + numkeys;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "LIMIT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                let limit_str = get_string(&args[i])?;
                limit = Some(limit_str.parse().map_err(|_| FerriteError::NotInteger)?);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::ZInterCard { keys, limit })
}

/// ZDIFF numkeys key [key ...] [WITHSCORES]
pub(crate) fn parse_zdiff(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("ZDIFF".to_string()));
    }

    let numkeys_str = get_string(&args[0])?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| FerriteError::NotInteger)?;

    if args.len() < 1 + numkeys {
        return Err(FerriteError::WrongArity("ZDIFF".to_string()));
    }

    let keys: Result<Vec<Bytes>> = args[1..=numkeys].iter().map(get_bytes).collect();
    let keys = keys?;

    let mut withscores = false;
    if args.len() > 1 + numkeys {
        let opt = get_string(&args[1 + numkeys])?.to_uppercase();
        if opt == "WITHSCORES" {
            withscores = true;
        } else {
            return Err(FerriteError::Syntax);
        }
    }

    Ok(Command::ZDiff { keys, withscores })
}

/// ZDIFFSTORE destination numkeys key [key ...]
pub(crate) fn parse_zdiffstore(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("ZDIFFSTORE".to_string()));
    }

    let destination = get_bytes(&args[0])?;
    let numkeys_str = get_string(&args[1])?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| FerriteError::NotInteger)?;

    if args.len() < 2 + numkeys {
        return Err(FerriteError::WrongArity("ZDIFFSTORE".to_string()));
    }

    let keys: Result<Vec<Bytes>> = args[2..2 + numkeys].iter().map(get_bytes).collect();
    let keys = keys?;

    Ok(Command::ZDiffStore { destination, keys })
}

/// ZRANGEBYLEX key min max [LIMIT offset count]
pub(crate) fn parse_zrangebylex(args: &[Frame]) -> Result<Command> {
    use crate::commands::sorted_sets::LexBound;

    if args.len() < 3 {
        return Err(FerriteError::WrongArity("ZRANGEBYLEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let min_str = get_string(&args[1])?;
    let max_str = get_string(&args[2])?;

    let min = LexBound::parse(&min_str).ok_or(FerriteError::Syntax)?;
    let max = LexBound::parse(&max_str).ok_or(FerriteError::Syntax)?;

    let mut offset = None;
    let mut count = None;

    let mut i = 3;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        if opt == "LIMIT" {
            if i + 2 >= args.len() {
                return Err(FerriteError::Syntax);
            }
            offset = Some(
                get_string(&args[i + 1])?
                    .parse()
                    .map_err(|_| FerriteError::NotInteger)?,
            );
            count = Some(
                get_string(&args[i + 2])?
                    .parse()
                    .map_err(|_| FerriteError::NotInteger)?,
            );
            i += 3;
        } else {
            return Err(FerriteError::Syntax);
        }
    }

    Ok(Command::ZRangeByLex {
        key,
        min,
        max,
        offset,
        count,
    })
}

/// ZREVRANGEBYLEX key max min [LIMIT offset count]
pub(crate) fn parse_zrevrangebylex(args: &[Frame]) -> Result<Command> {
    use crate::commands::sorted_sets::LexBound;

    if args.len() < 3 {
        return Err(FerriteError::WrongArity("ZREVRANGEBYLEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let max_str = get_string(&args[1])?;
    let min_str = get_string(&args[2])?;

    let max = LexBound::parse(&max_str).ok_or(FerriteError::Syntax)?;
    let min = LexBound::parse(&min_str).ok_or(FerriteError::Syntax)?;

    let mut offset = None;
    let mut count = None;

    let mut i = 3;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        if opt == "LIMIT" {
            if i + 2 >= args.len() {
                return Err(FerriteError::Syntax);
            }
            offset = Some(
                get_string(&args[i + 1])?
                    .parse()
                    .map_err(|_| FerriteError::NotInteger)?,
            );
            count = Some(
                get_string(&args[i + 2])?
                    .parse()
                    .map_err(|_| FerriteError::NotInteger)?,
            );
            i += 3;
        } else {
            return Err(FerriteError::Syntax);
        }
    }

    Ok(Command::ZRevRangeByLex {
        key,
        max,
        min,
        offset,
        count,
    })
}

/// ZLEXCOUNT key min max
pub(crate) fn parse_zlexcount(args: &[Frame]) -> Result<Command> {
    use crate::commands::sorted_sets::LexBound;

    if args.len() != 3 {
        return Err(FerriteError::WrongArity("ZLEXCOUNT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let min_str = get_string(&args[1])?;
    let max_str = get_string(&args[2])?;

    let min = LexBound::parse(&min_str).ok_or(FerriteError::Syntax)?;
    let max = LexBound::parse(&max_str).ok_or(FerriteError::Syntax)?;

    Ok(Command::ZLexCount { key, min, max })
}

/// ZREMRANGEBYRANK key start stop
pub(crate) fn parse_zremrangebyrank(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("ZREMRANGEBYRANK".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let start: i64 = get_string(&args[1])?
        .parse()
        .map_err(|_| FerriteError::NotInteger)?;
    let stop: i64 = get_string(&args[2])?
        .parse()
        .map_err(|_| FerriteError::NotInteger)?;

    Ok(Command::ZRemRangeByRank { key, start, stop })
}

/// ZREMRANGEBYSCORE key min max
pub(crate) fn parse_zremrangebyscore(args: &[Frame]) -> Result<Command> {
    if args.len() != 3 {
        return Err(FerriteError::WrongArity("ZREMRANGEBYSCORE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let min: f64 = parse_score(&get_string(&args[1])?)?;
    let max: f64 = parse_score(&get_string(&args[2])?)?;

    Ok(Command::ZRemRangeByScore { key, min, max })
}

/// ZREMRANGEBYLEX key min max
pub(crate) fn parse_zremrangebylex(args: &[Frame]) -> Result<Command> {
    use crate::commands::sorted_sets::LexBound;

    if args.len() != 3 {
        return Err(FerriteError::WrongArity("ZREMRANGEBYLEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let min_str = get_string(&args[1])?;
    let max_str = get_string(&args[2])?;

    let min = LexBound::parse(&min_str).ok_or(FerriteError::Syntax)?;
    let max = LexBound::parse(&max_str).ok_or(FerriteError::Syntax)?;

    Ok(Command::ZRemRangeByLex { key, min, max })
}

/// ZRANDMEMBER key [count [WITHSCORES]]
pub(crate) fn parse_zrandmember(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("ZRANDMEMBER".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut count = None;
    let mut withscores = false;

    if args.len() >= 2 {
        count = Some(
            get_string(&args[1])?
                .parse()
                .map_err(|_| FerriteError::NotInteger)?,
        );

        if args.len() >= 3 {
            let opt = get_string(&args[2])?.to_uppercase();
            if opt == "WITHSCORES" {
                withscores = true;
            } else {
                return Err(FerriteError::Syntax);
            }
        }
    }

    Ok(Command::ZRandMember {
        key,
        count,
        withscores,
    })
}

/// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
pub(crate) fn parse_zmpop(args: &[Frame]) -> Result<Command> {
    use crate::commands::sorted_sets::ZPopDirection;

    if args.is_empty() {
        return Err(FerriteError::WrongArity("ZMPOP".to_string()));
    }

    let numkeys_str = get_string(&args[0])?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| FerriteError::NotInteger)?;

    if args.len() < 1 + numkeys + 1 {
        return Err(FerriteError::WrongArity("ZMPOP".to_string()));
    }

    let keys: Result<Vec<Bytes>> = args[1..=numkeys].iter().map(get_bytes).collect();
    let keys = keys?;

    let direction_str = get_string(&args[1 + numkeys])?.to_uppercase();
    let direction = match direction_str.as_str() {
        "MIN" => ZPopDirection::Min,
        "MAX" => ZPopDirection::Max,
        _ => return Err(FerriteError::Syntax),
    };

    let mut count = None;
    let mut i = 2 + numkeys;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        if opt == "COUNT" {
            if i + 1 >= args.len() {
                return Err(FerriteError::Syntax);
            }
            count = Some(
                get_string(&args[i + 1])?
                    .parse()
                    .map_err(|_| FerriteError::NotInteger)?,
            );
            i += 2;
        } else {
            return Err(FerriteError::Syntax);
        }
    }

    Ok(Command::ZMPop {
        keys,
        direction,
        count,
    })
}

/// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
pub(crate) fn parse_bzmpop(args: &[Frame]) -> Result<Command> {
    use crate::commands::sorted_sets::ZPopDirection;

    if args.len() < 4 {
        return Err(FerriteError::WrongArity("BZMPOP".to_string()));
    }

    let timeout: f64 = get_string(&args[0])?
        .parse()
        .map_err(|_| FerriteError::NotFloat)?;
    let numkeys_str = get_string(&args[1])?;
    let numkeys: usize = numkeys_str.parse().map_err(|_| FerriteError::NotInteger)?;

    if args.len() < 2 + numkeys + 1 {
        return Err(FerriteError::WrongArity("BZMPOP".to_string()));
    }

    let keys: Result<Vec<Bytes>> = args[2..2 + numkeys].iter().map(get_bytes).collect();
    let keys = keys?;

    let direction_str = get_string(&args[2 + numkeys])?.to_uppercase();
    let direction = match direction_str.as_str() {
        "MIN" => ZPopDirection::Min,
        "MAX" => ZPopDirection::Max,
        _ => return Err(FerriteError::Syntax),
    };

    let mut count = None;
    let mut i = 3 + numkeys;
    while i < args.len() {
        let opt = get_string(&args[i])?.to_uppercase();
        if opt == "COUNT" {
            if i + 1 >= args.len() {
                return Err(FerriteError::Syntax);
            }
            count = Some(
                get_string(&args[i + 1])?
                    .parse()
                    .map_err(|_| FerriteError::NotInteger)?,
            );
            i += 2;
        } else {
            return Err(FerriteError::Syntax);
        }
    }

    Ok(Command::BZMPop {
        timeout,
        keys,
        direction,
        count,
    })
}

/// BZPOPMIN key [key ...] timeout
pub(crate) fn parse_bzpopmin(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("BZPOPMIN".to_string()));
    }

    let timeout: f64 = get_string(&args[args.len() - 1])?
        .parse()
        .map_err(|_| FerriteError::NotFloat)?;
    let keys: Result<Vec<Bytes>> = args[..args.len() - 1].iter().map(get_bytes).collect();
    let keys = keys?;

    Ok(Command::BZPopMin { keys, timeout })
}

/// BZPOPMAX key [key ...] timeout
pub(crate) fn parse_bzpopmax(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("BZPOPMAX".to_string()));
    }

    let timeout: f64 = get_string(&args[args.len() - 1])?
        .parse()
        .map_err(|_| FerriteError::NotFloat)?;
    let keys: Result<Vec<Bytes>> = args[..args.len() - 1].iter().map(get_bytes).collect();
    let keys = keys?;

    Ok(Command::BZPopMax { keys, timeout })
}
