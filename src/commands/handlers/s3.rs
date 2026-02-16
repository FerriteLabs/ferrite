//! S3 command handlers
//!
//! Implements S3-compatible commands for Ferrite.

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_cloud::s3::{S3Config, S3Server};

use super::{err_frame, ok_frame, HandlerContext};

/// Global S3 server instance
static S3_SERVER: OnceLock<S3Server> = OnceLock::new();

/// Get the global S3 server
fn s3_server() -> &'static S3Server {
    S3_SERVER.get_or_init(|| S3Server::new(S3Config::default()))
}

/// Handle S3.BUCKET.CREATE command
///
/// S3.BUCKET.CREATE bucket_name
pub fn handle_s3_bucket_create(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("S3.BUCKET.CREATE requires bucket name");
    }

    let bucket_name = String::from_utf8_lossy(&args[0]).to_string();
    let response = s3_server().create_bucket(&bucket_name);

    if response.is_success() {
        ok_frame()
    } else {
        err_frame("bucket creation failed")
    }
}

/// Handle S3.BUCKET.DELETE command
///
/// S3.BUCKET.DELETE bucket_name
pub fn handle_s3_bucket_delete(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("S3.BUCKET.DELETE requires bucket name");
    }

    let bucket_name = String::from_utf8_lossy(&args[0]).to_string();
    let response = s3_server().delete_bucket(&bucket_name);

    if response.is_success() {
        ok_frame()
    } else {
        err_frame("bucket deletion failed")
    }
}

/// Handle S3.BUCKET.LIST command
///
/// S3.BUCKET.LIST
pub fn handle_s3_bucket_list(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let response = s3_server().list_buckets();

    // Parse XML response to get bucket names
    let body = String::from_utf8_lossy(&response.body);

    // Simple extraction of bucket names from XML
    let buckets: Vec<Frame> = body
        .split("<Name>")
        .skip(1)
        .filter_map(|s| s.split("</Name>").next())
        .map(|name| Frame::bulk(name.to_string()))
        .collect();

    Frame::array(buckets)
}

/// Handle S3.OBJECT.PUT command
///
/// S3.OBJECT.PUT bucket key value [CONTENT-TYPE type]
pub fn handle_s3_object_put(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame("S3.OBJECT.PUT requires bucket, key, and value");
    }

    let bucket = String::from_utf8_lossy(&args[0]).to_string();
    let key = String::from_utf8_lossy(&args[1]).to_string();
    let data = args[2].to_vec();

    let mut metadata = HashMap::new();

    // Parse optional content-type
    let mut i = 3;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        if arg == "CONTENT-TYPE" && i + 1 < args.len() {
            i += 1;
            metadata.insert(
                "content-type".to_string(),
                String::from_utf8_lossy(&args[i]).to_string(),
            );
        }
        i += 1;
    }

    let response = s3_server().put_object(&bucket, &key, data, metadata);

    if response.is_success() {
        if let Some(etag) = response.headers.get("ETag") {
            Frame::bulk(etag.clone())
        } else {
            ok_frame()
        }
    } else {
        err_frame("object put failed")
    }
}

/// Handle S3.OBJECT.GET command
///
/// S3.OBJECT.GET bucket key
pub fn handle_s3_object_get(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("S3.OBJECT.GET requires bucket and key");
    }

    let bucket = String::from_utf8_lossy(&args[0]).to_string();
    let key = String::from_utf8_lossy(&args[1]).to_string();

    let response = s3_server().get_object(&bucket, &key);

    if response.is_success() {
        Frame::bulk(Bytes::from(response.body))
    } else {
        Frame::Null
    }
}

/// Handle S3.OBJECT.DELETE command
///
/// S3.OBJECT.DELETE bucket key
pub fn handle_s3_object_delete(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("S3.OBJECT.DELETE requires bucket and key");
    }

    let bucket = String::from_utf8_lossy(&args[0]).to_string();
    let key = String::from_utf8_lossy(&args[1]).to_string();

    let response = s3_server().delete_object(&bucket, &key);

    if response.is_success() {
        ok_frame()
    } else {
        err_frame("object deletion failed")
    }
}

/// Handle S3.OBJECT.HEAD command
///
/// S3.OBJECT.HEAD bucket key
pub fn handle_s3_object_head(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("S3.OBJECT.HEAD requires bucket and key");
    }

    let bucket = String::from_utf8_lossy(&args[0]).to_string();
    let key = String::from_utf8_lossy(&args[1]).to_string();

    let response = s3_server().head_object(&bucket, &key);

    if response.is_success() {
        let fields: Vec<Frame> = response
            .headers
            .iter()
            .flat_map(|(k, v)| vec![Frame::bulk(k.clone()), Frame::bulk(v.clone())])
            .collect();
        Frame::array(fields)
    } else {
        Frame::Null
    }
}

/// Handle S3.OBJECT.LIST command
///
/// S3.OBJECT.LIST bucket [PREFIX prefix] [MAX max_keys]
pub fn handle_s3_object_list(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("S3.OBJECT.LIST requires bucket name");
    }

    let bucket = String::from_utf8_lossy(&args[0]).to_string();
    let mut prefix_str: Option<String> = None;
    let mut max_keys: Option<usize> = None;

    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "PREFIX" if i + 1 < args.len() => {
                i += 1;
                prefix_str = Some(String::from_utf8_lossy(&args[i]).to_string());
            }
            "MAX" if i + 1 < args.len() => {
                i += 1;
                max_keys = String::from_utf8_lossy(&args[i]).parse().ok();
            }
            _ => {}
        }
        i += 1;
    }

    let response = s3_server().list_objects(&bucket, prefix_str.as_deref(), max_keys);

    if response.is_success() {
        // Parse XML response to extract object keys
        let body = String::from_utf8_lossy(&response.body);

        let objects: Vec<Frame> = body
            .split("<Key>")
            .skip(1)
            .filter_map(|s| {
                let key = s.split("</Key>").next()?;
                let size = s
                    .split("<Size>")
                    .nth(1)?
                    .split("</Size>")
                    .next()?
                    .parse::<i64>()
                    .ok()?;

                Some(Frame::array(vec![
                    Frame::bulk(key.to_string()),
                    Frame::Integer(size),
                ]))
            })
            .collect();

        Frame::array(objects)
    } else {
        Frame::array(vec![])
    }
}

/// Handle S3.STATS command
///
/// S3.STATS
pub fn handle_s3_stats(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let stats = s3_server().stats();

    Frame::array(vec![
        Frame::bulk("bucket_count"),
        Frame::Integer(stats.bucket_count as i64),
        Frame::bulk("total_objects"),
        Frame::Integer(stats.total_objects as i64),
        Frame::bulk("total_size"),
        Frame::Integer(stats.total_size as i64),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_server_singleton() {
        let s1 = s3_server();
        let s2 = s3_server();
        assert!(std::ptr::eq(s1, s2));
    }
}
