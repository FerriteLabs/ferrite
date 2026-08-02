//! Search and semantic caching command parsers (FT.*, SEMANTIC.*, VECTOR.*).

use super::{get_bytes, get_float, get_int, get_string};
use crate::commands::parser::Command;
use crate::error::{FerriteError, Result};
use crate::protocol::Frame;

pub(crate) fn parse_ft_create(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("FT.CREATE".to_string()));
    }

    let index = get_bytes(&args[0])?;
    let mut schema = Vec::new();
    let mut index_type = None;
    let mut dimension = None;
    let mut metric = None;

    let mut i = 1;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "SCHEMA" => {
                i += 1;
                // Parse field definitions
                while i + 1 < args.len() {
                    let field_name = get_string(&args[i])?;
                    let field_type = get_string(&args[i + 1])?.to_uppercase();

                    // Check if this is a keyword rather than field type
                    if field_type == "VECTOR"
                        || field_type == "TEXT"
                        || field_type == "NUMERIC"
                        || field_type == "TAG"
                    {
                        schema.push((field_name, field_type));
                        i += 2;
                    } else {
                        break;
                    }
                }
            }
            "TYPE" => {
                i += 1;
                if i < args.len() {
                    index_type = Some(get_string(&args[i])?);
                    i += 1;
                }
            }
            "DIM" | "DIMENSION" => {
                i += 1;
                if i < args.len() {
                    dimension = Some(get_int(&args[i])? as usize);
                    i += 1;
                }
            }
            "DISTANCE_METRIC" | "METRIC" => {
                i += 1;
                if i < args.len() {
                    metric = Some(get_string(&args[i])?);
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(Command::FtCreate {
        index,
        schema,
        index_type,
        dimension,
        metric,
    })
}

pub(crate) fn parse_ft_dropindex(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("FT.DROPINDEX".to_string()));
    }

    let index = get_bytes(&args[0])?;
    let mut delete_docs = false;

    for arg in &args[1..] {
        let s = get_string(arg)?.to_uppercase();
        if s == "DD" {
            delete_docs = true;
        }
    }

    Ok(Command::FtDropIndex { index, delete_docs })
}

pub(crate) fn parse_ft_add(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("FT.ADD".to_string()));
    }

    let index = get_bytes(&args[0])?;
    let key = get_bytes(&args[1])?;

    // Parse vector - can be comma-separated string or array of numbers
    let vector_str = get_string(&args[2])?;
    let vector: Vec<f32> = vector_str
        .split([',', ' '])
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let mut payload = None;
    let mut i = 3;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        if arg == "PAYLOAD" && i + 1 < args.len() {
            payload = Some(get_bytes(&args[i + 1])?);
            i += 2;
        } else {
            i += 1;
        }
    }

    Ok(Command::FtAdd {
        index,
        key,
        vector,
        payload,
    })
}

pub(crate) fn parse_ft_del(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("FT.DEL".to_string()));
    }

    let index = get_bytes(&args[0])?;
    let key = get_bytes(&args[1])?;

    Ok(Command::FtDel { index, key })
}

pub(crate) fn parse_ft_search(args: &[Frame]) -> Result<Command> {
    if args.len() < 2 {
        return Err(FerriteError::WrongArity("FT.SEARCH".to_string()));
    }

    let index = get_bytes(&args[0])?;

    // Parse query vector
    let query_str = get_string(&args[1])?;
    let query: Vec<f32> = query_str
        .split([',', ' '])
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let mut k = 10; // default
    let mut return_fields = Vec::new();
    let mut filter = None;

    let mut i = 2;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "KNN" | "K" => {
                i += 1;
                if i < args.len() {
                    k = get_int(&args[i])? as usize;
                    i += 1;
                }
            }
            "RETURN" => {
                i += 1;
                if i < args.len() {
                    let count = get_int(&args[i])? as usize;
                    i += 1;
                    for _ in 0..count {
                        if i < args.len() {
                            return_fields.push(get_string(&args[i])?);
                            i += 1;
                        }
                    }
                }
            }
            "FILTER" => {
                i += 1;
                if i < args.len() {
                    filter = Some(get_string(&args[i])?);
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(Command::FtSearch {
        index,
        query,
        k,
        return_fields,
        filter,
    })
}

pub(crate) fn parse_ft_info(args: &[Frame]) -> Result<Command> {
    if args.is_empty() {
        return Err(FerriteError::WrongArity("FT.INFO".to_string()));
    }

    let index = get_bytes(&args[0])?;
    Ok(Command::FtInfo { index })
}

pub(crate) fn parse_semantic_set(args: &[Frame]) -> Result<Command> {
    // SEMANTIC.SET query value embedding [EX seconds] [THRESHOLD threshold]
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("SEMANTIC.SET".to_string()));
    }

    let query = get_bytes(&args[0])?;
    let value = get_bytes(&args[1])?;

    // Parse embedding - it could be a space-separated string or array of floats
    let embedding = parse_embedding(&args[2])?;

    let mut ttl_secs = None;
    let mut threshold = None;

    let mut i = 3;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                ttl_secs = Some(get_int(&args[i])? as u64);
            }
            "THRESHOLD" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                threshold = Some(get_float(&args[i])? as f32);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::SemanticSet {
        query,
        value,
        embedding,
        ttl_secs,
        threshold,
    })
}

pub(crate) fn parse_semantic_get(args: &[Frame]) -> Result<Command> {
    // SEMANTIC.GET embedding [THRESHOLD threshold] [COUNT count]
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SEMANTIC.GET".to_string()));
    }

    let embedding = parse_embedding(&args[0])?;

    let mut threshold = None;
    let mut count = None;

    let mut i = 1;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "THRESHOLD" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                threshold = Some(get_float(&args[i])? as f32);
            }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                count = Some(get_int(&args[i])? as usize);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::SemanticGet {
        embedding,
        threshold,
        count,
    })
}

pub(crate) fn parse_semantic_gettext(args: &[Frame]) -> Result<Command> {
    // SEMANTIC.GETTEXT query [THRESHOLD threshold] [COUNT count]
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SEMANTIC.GETTEXT".to_string()));
    }

    let query = get_bytes(&args[0])?;

    let mut threshold = None;
    let mut count = None;

    let mut i = 1;
    while i < args.len() {
        let option = get_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "THRESHOLD" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                threshold = Some(get_float(&args[i])? as f32);
            }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(FerriteError::Syntax);
                }
                count = Some(get_int(&args[i])? as usize);
            }
            _ => return Err(FerriteError::Syntax),
        }
        i += 1;
    }

    Ok(Command::SemanticGetText {
        query,
        threshold,
        count,
    })
}

pub(crate) fn parse_semantic_del(args: &[Frame]) -> Result<Command> {
    // SEMANTIC.DEL id
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SEMANTIC.DEL".to_string()));
    }

    let id = get_int(&args[0])? as u64;
    Ok(Command::SemanticDel { id })
}

pub(crate) fn parse_semantic_config(args: &[Frame]) -> Result<Command> {
    // SEMANTIC.CONFIG GET|SET [param] [value]
    if args.is_empty() {
        return Err(FerriteError::WrongArity("SEMANTIC.CONFIG".to_string()));
    }

    let operation = get_bytes(&args[0])?;
    let param = args.get(1).map(get_bytes).transpose()?;
    let value = args.get(2).map(get_bytes).transpose()?;

    Ok(Command::SemanticConfig {
        operation,
        param,
        value,
    })
}

/// Parse an embedding vector from a frame
/// Supports: space-separated string of floats, or array of floats
pub(crate) fn parse_embedding(frame: &Frame) -> Result<Vec<f32>> {
    match frame {
        Frame::Simple(b) | Frame::Bulk(Some(b)) => {
            // Parse space-separated or comma-separated floats
            let s = String::from_utf8(b.to_vec())
                .map_err(|_| FerriteError::Protocol("invalid UTF-8".to_string()))?;

            let floats: Result<Vec<f32>> = s
                .split(|c: char| c.is_whitespace() || c == ',')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    s.parse::<f32>()
                        .map_err(|_| FerriteError::Protocol(format!("invalid float: {}", s)))
                })
                .collect();

            floats
        }
        Frame::Array(Some(frames)) => {
            // Parse array of floats
            frames
                .iter()
                .map(|f| match f {
                    Frame::Simple(b) | Frame::Bulk(Some(b)) => {
                        let s = String::from_utf8(b.to_vec())
                            .map_err(|_| FerriteError::Protocol("invalid UTF-8".to_string()))?;
                        s.parse::<f32>()
                            .map_err(|_| FerriteError::Protocol(format!("invalid float: {}", s)))
                    }
                    Frame::Integer(i) => Ok(*i as f32),
                    Frame::Double(d) => Ok(*d as f32),
                    _ => Err(FerriteError::Protocol("expected float".to_string())),
                })
                .collect()
        }
        _ => Err(FerriteError::Protocol(
            "expected embedding vector".to_string(),
        )),
    }
}

pub(crate) fn parse_vector_hybrid(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("VECTOR.HYBRID".to_string()));
    }

    let index = get_bytes(&args[0])?;

    // Parse query vector (comma-separated floats)
    let vec_str = get_string(&args[1])?;
    let query_vector: Vec<f32> = vec_str
        .split([',', ' '])
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let query_text = get_string(&args[2])?;

    let mut top_k = 10usize;
    let mut alpha = 0.5f64;
    let mut strategy = "rrf".to_string();

    let mut i = 3;
    while i < args.len() {
        let arg = get_string(&args[i])?.to_uppercase();
        match arg.as_str() {
            "TOP" | "K" => {
                i += 1;
                if i < args.len() {
                    top_k = get_int(&args[i])? as usize;
                    i += 1;
                }
            }
            "ALPHA" => {
                i += 1;
                if i < args.len() {
                    alpha = get_float(&args[i])?;
                    i += 1;
                }
            }
            "STRATEGY" => {
                i += 1;
                if i < args.len() {
                    strategy = get_string(&args[i])?.to_lowercase();
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(Command::VectorHybridSearch {
        index,
        query_vector,
        query_text,
        top_k,
        alpha,
        strategy,
    })
}

pub(crate) fn parse_vector_rerank(args: &[Frame]) -> Result<Command> {
    if args.len() < 3 {
        return Err(FerriteError::WrongArity("VECTOR.RERANK".to_string()));
    }

    let index = get_bytes(&args[0])?;
    let query_text = get_string(&args[1])?;

    let mut doc_ids = Vec::new();
    let mut top_k = 10usize;

    let mut i = 2;
    while i < args.len() {
        let arg = get_string(&args[i])?;
        if arg.to_uppercase() == "TOP" {
            i += 1;
            if i < args.len() {
                top_k = get_int(&args[i])? as usize;
                i += 1;
            }
        } else {
            doc_ids.push(arg);
            i += 1;
        }
    }

    Ok(Command::VectorRerank {
        index,
        query_text,
        doc_ids,
        top_k,
    })
}
