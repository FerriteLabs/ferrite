//! RESP protocol client for connecting to Ferrite/Redis servers

use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// A frame representing a RESP protocol value
#[derive(Debug, Clone)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Frame>>),
    Null,
}

impl Frame {
    /// Check if this frame is an error
    pub fn is_error(&self) -> bool {
        matches!(self, Frame::Error(_))
    }
}

/// Client for connecting to Ferrite/Redis servers
pub struct FerriteClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl FerriteClient {
    /// Connect to a Ferrite/Redis server
    pub async fn connect(host: &str, port: u16) -> io::Result<Self> {
        let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        let (read_half, write_half) = stream.into_split();
        let reader = BufReader::new(read_half);

        Ok(Self {
            reader,
            writer: write_half,
        })
    }

    /// Authenticate with the server
    pub async fn authenticate(&mut self, user: Option<&str>, password: &str) -> io::Result<()> {
        let response = if let Some(username) = user {
            self.send_command(&["AUTH", username, password]).await?
        } else {
            self.send_command(&["AUTH", password]).await?
        };

        if response.is_error() {
            if let Frame::Error(msg) = response {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, msg));
            }
        }
        Ok(())
    }

    /// Select a database
    pub async fn select(&mut self, db: u8) -> io::Result<()> {
        let response = self.send_command(&["SELECT", &db.to_string()]).await?;
        if response.is_error() {
            if let Frame::Error(msg) = response {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
            }
        }
        Ok(())
    }

    /// Send a command and receive the response
    pub async fn send_command(&mut self, args: &[&str]) -> io::Result<Frame> {
        // Encode command as RESP array
        let mut cmd = format!("*{}\r\n", args.len());
        for arg in args {
            cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }

        // Send command
        self.writer.write_all(cmd.as_bytes()).await?;
        self.writer.flush().await?;

        // Read response
        self.read_frame().await
    }

    /// Read a RESP frame from the connection
    async fn read_frame(&mut self) -> io::Result<Frame> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;

        if line.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "Connection closed",
            ));
        }

        let line = line.trim_end_matches("\r\n").trim_end_matches('\n');

        match line.chars().next() {
            Some('+') => Ok(Frame::Simple(line[1..].to_string())),
            Some('-') => Ok(Frame::Error(line[1..].to_string())),
            Some(':') => {
                let n: i64 = line[1..]
                    .parse()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid integer"))?;
                Ok(Frame::Integer(n))
            }
            Some('$') => {
                let len: i64 = line[1..].parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk length")
                })?;

                if len < 0 {
                    return Ok(Frame::Null);
                }

                let len = len as usize;
                let mut buf = vec![0u8; len + 2]; // +2 for \r\n
                tokio::io::AsyncReadExt::read_exact(&mut self.reader, &mut buf).await?;
                buf.truncate(len); // Remove \r\n
                Ok(Frame::Bulk(Some(buf)))
            }
            Some('*') => {
                let count: i64 = line[1..].parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid array length")
                })?;

                if count < 0 {
                    return Ok(Frame::Array(None));
                }

                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    items.push(Box::pin(self.read_frame()).await?);
                }
                Ok(Frame::Array(Some(items)))
            }
            Some('_') => Ok(Frame::Null),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown RESP type: {}", line),
            )),
        }
    }
}
