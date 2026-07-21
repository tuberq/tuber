use std::io;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub struct TuberClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

pub enum ReserveResult {
    Reserved { id: u64, body: Vec<u8> },
    TimedOut,
    Error(String),
}

/// Reject a tube name or tag value that would break the line-based protocol.
/// Whitespace or control bytes (notably CR/LF) let a caller inject a second
/// command or desync the stream, so they're refused before anything is sent.
fn validate_token(kind: &str, s: &str) -> io::Result<()> {
    if s.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{kind} must not be empty"),
        ));
    }
    if s.bytes().any(|b| b <= b' ' || b == 0x7f) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{kind} contains whitespace or control characters: {s:?}"),
        ));
    }
    Ok(())
}

impl TuberClient {
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(reader),
            writer,
        })
    }

    pub async fn use_tube(&mut self, tube: &str) -> io::Result<String> {
        validate_token("tube name", tube)?;
        self.send_line(&format!("use {tube}")).await?;
        self.read_line().await
    }

    pub async fn watch(&mut self, tube: &str) -> io::Result<String> {
        validate_token("tube name", tube)?;
        self.send_line(&format!("watch {tube}")).await?;
        self.read_line().await
    }

    pub async fn ignore(&mut self, tube: &str) -> io::Result<String> {
        validate_token("tube name", tube)?;
        self.send_line(&format!("ignore {tube}")).await?;
        self.read_line().await
    }

    pub async fn put(
        &mut self,
        pri: u32,
        delay: u32,
        ttr: u32,
        body: &[u8],
        idempotency_key: Option<&str>,
        group: Option<&str>,
        after_group: Option<&str>,
        concurrency_key: Option<&str>,
    ) -> io::Result<String> {
        let mut cmd = format!("put {} {} {} {}", pri, delay, ttr, body.len());
        if let Some(key) = idempotency_key {
            validate_token("idp: value", key)?;
            cmd.push_str(&format!(" idp:{key}"));
        }
        if let Some(key) = group {
            validate_token("grp: value", key)?;
            cmd.push_str(&format!(" grp:{key}"));
        }
        if let Some(key) = after_group {
            validate_token("aft: value", key)?;
            cmd.push_str(&format!(" aft:{key}"));
        }
        if let Some(key) = concurrency_key {
            validate_token("con: value", key)?;
            cmd.push_str(&format!(" con:{key}"));
        }
        cmd.push_str("\r\n");
        self.writer.write_all(cmd.as_bytes()).await?;
        self.writer.write_all(body).await?;
        self.writer.write_all(b"\r\n").await?;
        self.writer.flush().await?;
        self.read_line().await
    }

    pub async fn reserve_with_timeout(&mut self, timeout: u32) -> io::Result<ReserveResult> {
        self.send_line(&format!("reserve-with-timeout {timeout}"))
            .await?;
        let line = self.read_line().await?;
        if line.starts_with("RESERVED") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            // Guard before indexing: a truncated/malformed "RESERVED" reply
            // (fewer than 3 fields) must surface as an error, not panic.
            if parts.len() < 3 {
                return Err(io::Error::other(format!("malformed RESERVED reply: {line}")));
            }
            let id: u64 = parts[1]
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let bytes: usize = parts[2]
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut buf = vec![0u8; bytes + 2]; // +2 for trailing \r\n
            self.reader.read_exact(&mut buf).await?;
            buf.truncate(bytes);
            Ok(ReserveResult::Reserved { id, body: buf })
        } else if line.starts_with("TIMED_OUT") {
            Ok(ReserveResult::TimedOut)
        } else {
            Ok(ReserveResult::Error(line))
        }
    }

    pub async fn reserve_batch(&mut self, count: u32) -> io::Result<Vec<(u64, Vec<u8>)>> {
        self.send_line(&format!("reserve-batch {count}")).await?;
        let line = self.read_line().await?;
        let n: usize = line
            .strip_prefix("RESERVED_BATCH ")
            .ok_or_else(|| io::Error::other(line.clone()))?
            .trim()
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut jobs = Vec::with_capacity(n);
        for _ in 0..n {
            let rline = self.read_line().await?;
            let parts: Vec<&str> = rline.split_whitespace().collect();
            if parts.len() != 3 || parts[0] != "RESERVED" {
                return Err(io::Error::other(format!("unexpected: {rline}")));
            }
            let id: u64 = parts[1]
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let bytes: usize = parts[2]
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut buf = vec![0u8; bytes + 2];
            self.reader.read_exact(&mut buf).await?;
            buf.truncate(bytes);
            jobs.push((id, buf));
        }
        Ok(jobs)
    }

    pub async fn delete_batch(&mut self, ids: &[u64]) -> io::Result<(u32, u32)> {
        let id_strs: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        self.send_line(&format!("delete-batch {}", id_strs.join(" ")))
            .await?;
        let line = self.read_line().await?;
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 3 || parts[0] != "DELETED_BATCH" {
            return Err(io::Error::other(line));
        }
        let deleted: u32 = parts[1]
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let not_found: u32 = parts[2]
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok((deleted, not_found))
    }

    pub async fn delete(&mut self, id: u64) -> io::Result<String> {
        self.send_line(&format!("delete {id}")).await?;
        self.read_line().await
    }

    pub async fn bury(&mut self, id: u64, pri: u32) -> io::Result<String> {
        self.send_line(&format!("bury {id} {pri}")).await?;
        self.read_line().await
    }

    pub async fn release(&mut self, id: u64, pri: u32, delay: u32) -> io::Result<String> {
        self.send_line(&format!("release {id} {pri} {delay}"))
            .await?;
        self.read_line().await
    }

    pub async fn touch(&mut self, id: u64) -> io::Result<String> {
        self.send_line(&format!("touch {id}")).await?;
        self.read_line().await
    }

    /// Extend the TTR of every job this connection holds. Returns the count
    /// that were still held. Cheaper than one `touch` per job for a worker
    /// draining a `reserve-batch` window, and needs no id bookkeeping.
    pub async fn touch_all(&mut self) -> io::Result<u32> {
        self.send_line("touch-all").await?;
        let line = self.read_line().await?;
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 2 || parts[0] != "TOUCHED_ALL" {
            return Err(io::Error::other(line));
        }
        parts[1]
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub async fn stats_job(&mut self, id: u64) -> io::Result<String> {
        self.send_line(&format!("stats-job {id}")).await?;
        self.read_ok_body().await
    }

    pub async fn stats(&mut self) -> io::Result<String> {
        self.send_line("stats").await?;
        self.read_ok_body().await
    }

    pub async fn stats_tube(&mut self, tube: &str) -> io::Result<String> {
        validate_token("tube name", tube)?;
        self.send_line(&format!("stats-tube {tube}")).await?;
        self.read_ok_body().await
    }

    pub async fn list_tubes(&mut self) -> io::Result<String> {
        self.send_line("list-tubes").await?;
        self.read_ok_body().await
    }

    /// Read an `OK <bytes>\r\n<body>\r\n` response and return the body.
    async fn read_ok_body(&mut self) -> io::Result<String> {
        let line = self.read_line().await?;
        if !line.starts_with("OK ") {
            return Err(io::Error::other(line));
        }
        let bytes: usize = line[3..]
            .trim()
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut buf = vec![0u8; bytes + 2]; // +2 for trailing \r\n
        self.reader.read_exact(&mut buf).await?;
        buf.truncate(bytes);
        String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn send_line(&mut self, line: &str) -> io::Result<()> {
        self.writer
            .write_all(format!("{line}\r\n").as_bytes())
            .await?;
        self.writer.flush().await
    }

    async fn read_line(&mut self) -> io::Result<String> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;
        if line.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        Ok(line.trim_end().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::validate_token;

    #[test]
    fn validate_token_accepts_plain_and_colon_forms() {
        assert!(validate_token("tube name", "orders").is_ok());
        // con:/idp: values legitimately carry a colon (key:N).
        assert!(validate_token("con: value", "api:5").is_ok());
    }

    #[test]
    fn validate_token_rejects_injection_and_empty() {
        assert!(validate_token("tube name", "").is_err());
        assert!(validate_token("tube name", "a b").is_err());
        assert!(validate_token("tube name", "a\r\nDELETE 1").is_err());
        assert!(validate_token("tube name", "a\tb").is_err());
        assert!(validate_token("tube name", "a\n").is_err());
    }
}
