use crate::job::MAX_TUBE_NAME_LEN;

const NAME_CHARS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-+/;.$_()";

/// Commands parsed from the beanstalkd text protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Put {
        pri: u32,
        delay: u32,
        ttr: u32,
        bytes: u32,
        idempotency_key: Option<String>,
        group: Option<String>,
        after_group: Option<String>,
        concurrency_key: Option<String>,
    },
    Use {
        tube: String,
    },
    Reserve,
    ReserveWithTimeout {
        timeout: u32,
    },
    ReserveJob {
        id: u64,
    },
    ReserveMode {
        mode: String,
    },
    Delete {
        id: u64,
    },
    Release {
        id: u64,
        pri: u32,
        delay: u32,
    },
    Bury {
        id: u64,
        pri: u32,
    },
    Touch {
        id: u64,
    },
    Watch {
        tube: String,
        weight: u32,
    },
    Ignore {
        tube: String,
    },
    Peek {
        id: u64,
    },
    PeekReady,
    PeekDelayed,
    PeekBuried,
    Kick {
        bound: u32,
    },
    KickJob {
        id: u64,
    },
    StatsJob {
        id: u64,
    },
    StatsTube {
        tube: String,
    },
    Stats,
    ListTubes,
    ListTubeUsed,
    ListTubesWatched,
    PauseTube {
        tube: String,
        delay: u32,
    },
    FlushTube {
        tube: String,
    },
    Drain,
    Quit,
}

/// Responses sent back to the client.
#[derive(Debug, Clone)]
pub enum Response {
    Inserted(u64),
    BuriedId(u64),
    Buried,
    Using(String),
    Reserved { id: u64, body: Vec<u8> },
    DeadlineSoon,
    TimedOut,
    Deleted,
    Released,
    Touched,
    Kicked(u32),
    KickedOne,
    Found { id: u64, body: Vec<u8> },
    NotFound,
    Watching(usize),
    NotIgnored,
    Ok(Vec<u8>),
    Paused,
    Flushed(u32),
    OutOfMemory,
    InternalError,
    Draining,
    BadFormat,
    UnknownCommand,
    ExpectedCrlf,
    JobTooBig,
}

/// Serialize a response header + body payload (header\r\nbody\r\n).
fn serialize_with_body(header: String, body: &[u8]) -> Vec<u8> {
    let mut out = header.into_bytes();
    out.extend_from_slice(body);
    out.extend_from_slice(b"\r\n");
    out
}

impl Response {
    /// Serialize the response to bytes for sending over TCP.
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Response::Inserted(id) => format!("INSERTED {id}\r\n").into_bytes(),
            Response::BuriedId(id) => format!("BURIED {id}\r\n").into_bytes(),
            Response::Buried => b"BURIED\r\n".to_vec(),
            Response::Using(name) => format!("USING {name}\r\n").into_bytes(),
            Response::Reserved { id, body } => {
                serialize_with_body(format!("RESERVED {id} {}\r\n", body.len()), body)
            }
            Response::DeadlineSoon => b"DEADLINE_SOON\r\n".to_vec(),
            Response::TimedOut => b"TIMED_OUT\r\n".to_vec(),
            Response::Deleted => b"DELETED\r\n".to_vec(),
            Response::Released => b"RELEASED\r\n".to_vec(),
            Response::Touched => b"TOUCHED\r\n".to_vec(),
            Response::Kicked(n) => format!("KICKED {n}\r\n").into_bytes(),
            Response::KickedOne => b"KICKED\r\n".to_vec(),
            Response::Found { id, body } => {
                serialize_with_body(format!("FOUND {id} {}\r\n", body.len()), body)
            }
            Response::NotFound => b"NOT_FOUND\r\n".to_vec(),
            Response::Watching(n) => format!("WATCHING {n}\r\n").into_bytes(),
            Response::NotIgnored => b"NOT_IGNORED\r\n".to_vec(),
            Response::Ok(data) => serialize_with_body(format!("OK {}\r\n", data.len()), data),
            Response::Paused => b"PAUSED\r\n".to_vec(),
            Response::Flushed(n) => format!("FLUSHED {n}\r\n").into_bytes(),
            Response::OutOfMemory => b"OUT_OF_MEMORY\r\n".to_vec(),
            Response::InternalError => b"INTERNAL_ERROR\r\n".to_vec(),
            Response::Draining => b"DRAINING\r\n".to_vec(),
            Response::BadFormat => b"BAD_FORMAT\r\n".to_vec(),
            Response::UnknownCommand => b"UNKNOWN_COMMAND\r\n".to_vec(),
            Response::ExpectedCrlf => b"EXPECTED_CRLF\r\n".to_vec(),
            Response::JobTooBig => b"JOB_TOO_BIG\r\n".to_vec(),
        }
    }
}

fn is_valid_tube_name(name: &str) -> bool {
    let len = name.len();
    len > 0
        && len <= MAX_TUBE_NAME_LEN
        && name.bytes().all(|b| NAME_CHARS.as_bytes().contains(&b))
        && !name.starts_with('-')
}

/// Parse a single command line (without the trailing \r\n).
pub fn parse_command(line: &str) -> Result<Command, Response> {
    // The line has already been stripped of \r\n by the caller.
    let line = line.trim_end_matches('\0');

    // Check for embedded NULs (malicious input)
    if line.bytes().any(|b| b == 0) {
        return Err(Response::BadFormat);
    }

    if let Some(rest) = line.strip_prefix("put ") {
        parse_put(rest)
    } else if let Some(rest) = line.strip_prefix("peek ") {
        parse_peek(rest)
    } else if line == "peek-ready" {
        Ok(Command::PeekReady)
    } else if line == "peek-delayed" {
        Ok(Command::PeekDelayed)
    } else if line == "peek-buried" {
        Ok(Command::PeekBuried)
    } else if let Some(rest) = line.strip_prefix("reserve-mode ") {
        Ok(Command::ReserveMode {
            mode: rest.to_string(),
        })
    } else if let Some(rest) = line.strip_prefix("reserve-with-timeout ") {
        parse_reserve_with_timeout(rest)
    } else if let Some(rest) = line.strip_prefix("reserve-job ") {
        parse_reserve_job(rest)
    } else if line == "reserve" {
        Ok(Command::Reserve)
    } else if let Some(rest) = line.strip_prefix("delete ") {
        parse_uint(rest).map(|id| Command::Delete { id })
    } else if let Some(rest) = line.strip_prefix("release ") {
        parse_release(rest)
    } else if let Some(rest) = line.strip_prefix("bury ") {
        parse_bury(rest)
    } else if let Some(rest) = line.strip_prefix("kick-job ") {
        parse_uint(rest).map(|id| Command::KickJob { id })
    } else if let Some(rest) = line.strip_prefix("kick ") {
        parse_uint(rest).map(|bound| Command::Kick { bound })
    } else if let Some(rest) = line.strip_prefix("touch ") {
        parse_uint(rest).map(|id| Command::Touch { id })
    } else if let Some(rest) = line.strip_prefix("stats-job ") {
        parse_uint(rest).map(|id| Command::StatsJob { id })
    } else if let Some(rest) = line.strip_prefix("stats-tube ") {
        parse_stats_tube(rest)
    } else if line == "stats" {
        Ok(Command::Stats)
    } else if let Some(rest) = line.strip_prefix("use ") {
        parse_use(rest)
    } else if let Some(rest) = line.strip_prefix("watch ") {
        parse_watch(rest)
    } else if let Some(rest) = line.strip_prefix("ignore ") {
        parse_ignore(rest)
    } else if line == "list-tubes-watched" {
        Ok(Command::ListTubesWatched)
    } else if line == "list-tube-used" {
        Ok(Command::ListTubeUsed)
    } else if line == "list-tubes" {
        Ok(Command::ListTubes)
    } else if line == "quit" {
        Ok(Command::Quit)
    } else if line.starts_with("pause-tube") {
        parse_pause_tube(line.strip_prefix("pause-tube").unwrap())
    } else if let Some(rest) = line.strip_prefix("flush-tube ") {
        parse_flush_tube(rest)
    } else if line == "drain" {
        Ok(Command::Drain)
    } else {
        Err(Response::UnknownCommand)
    }
}

fn parse_uint<T: std::str::FromStr>(s: &str) -> Result<T, Response> {
    let s = s.trim();
    if s.is_empty() || s.starts_with('-') {
        return Err(Response::BadFormat);
    }
    s.parse().map_err(|_| Response::BadFormat)
}

fn is_valid_key(s: &str) -> bool {
    is_valid_tube_name(s)
}

fn parse_put(rest: &str) -> Result<Command, Response> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() < 4 {
        return Err(Response::BadFormat);
    }
    let pri = parts[0].parse::<u32>().map_err(|_| Response::BadFormat)?;
    let delay = parts[1].parse::<u32>().map_err(|_| Response::BadFormat)?;
    let ttr = parts[2].parse::<u32>().map_err(|_| Response::BadFormat)?;
    let bytes = parts[3].parse::<u32>().map_err(|_| Response::BadFormat)?;

    let mut idempotency_key = None;
    let mut group = None;
    let mut after_group = None;
    let mut concurrency_key = None;

    for part in &parts[4..] {
        if let Some(val) = part.strip_prefix("idp:") {
            if !is_valid_key(val) {
                return Err(Response::BadFormat);
            }
            idempotency_key = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("grp:") {
            if !is_valid_key(val) {
                return Err(Response::BadFormat);
            }
            group = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("aft:") {
            if !is_valid_key(val) {
                return Err(Response::BadFormat);
            }
            after_group = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("con:") {
            if !is_valid_key(val) {
                return Err(Response::BadFormat);
            }
            concurrency_key = Some(val.to_string());
        } else {
            return Err(Response::BadFormat);
        }
    }

    Ok(Command::Put {
        pri,
        delay,
        ttr,
        bytes,
        idempotency_key,
        group,
        after_group,
        concurrency_key,
    })
}

fn parse_peek(rest: &str) -> Result<Command, Response> {
    parse_uint(rest).map(|id| Command::Peek { id })
}

fn parse_reserve_with_timeout(rest: &str) -> Result<Command, Response> {
    parse_uint(rest).map(|timeout| Command::ReserveWithTimeout { timeout })
}

fn parse_reserve_job(rest: &str) -> Result<Command, Response> {
    parse_uint(rest).map(|id| Command::ReserveJob { id })
}

fn parse_release(rest: &str) -> Result<Command, Response> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() != 3 {
        return Err(Response::BadFormat);
    }
    let id = parts[0].parse::<u64>().map_err(|_| Response::BadFormat)?;
    let pri = parts[1].parse::<u32>().map_err(|_| Response::BadFormat)?;
    let delay = parts[2].parse::<u32>().map_err(|_| Response::BadFormat)?;
    Ok(Command::Release { id, pri, delay })
}

fn parse_bury(rest: &str) -> Result<Command, Response> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(Response::BadFormat);
    }
    let id = parts[0].parse::<u64>().map_err(|_| Response::BadFormat)?;
    let pri = parts[1].parse::<u32>().map_err(|_| Response::BadFormat)?;
    Ok(Command::Bury { id, pri })
}

fn parse_use(rest: &str) -> Result<Command, Response> {
    let name = rest.trim();
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    Ok(Command::Use {
        tube: name.to_string(),
    })
}

fn parse_watch(rest: &str) -> Result<Command, Response> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.is_empty() || parts.len() > 2 {
        return Err(Response::BadFormat);
    }
    let name = parts[0];
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    let weight = if parts.len() == 2 {
        let w = parts[1].parse::<u32>().map_err(|_| Response::BadFormat)?;
        if w == 0 || w > crate::conn::MAX_TUBE_WEIGHT {
            return Err(Response::BadFormat);
        }
        w
    } else {
        1
    };
    Ok(Command::Watch {
        tube: name.to_string(),
        weight,
    })
}

fn parse_ignore(rest: &str) -> Result<Command, Response> {
    let name = rest.trim();
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    Ok(Command::Ignore {
        tube: name.to_string(),
    })
}

fn parse_stats_tube(rest: &str) -> Result<Command, Response> {
    let name = rest.trim();
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    Ok(Command::StatsTube {
        tube: name.to_string(),
    })
}

fn parse_flush_tube(rest: &str) -> Result<Command, Response> {
    let name = rest.trim();
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    Ok(Command::FlushTube {
        tube: name.to_string(),
    })
}

fn parse_pause_tube(rest: &str) -> Result<Command, Response> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(Response::BadFormat);
    }
    let name = parts[0];
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    let delay = parts[1].parse::<u32>().map_err(|_| Response::BadFormat)?;
    Ok(Command::PauseTube {
        tube: name.to_string(),
        delay,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_cmd(pri: u32, delay: u32, ttr: u32, bytes: u32) -> Command {
        Command::Put {
            pri,
            delay,
            ttr,
            bytes,
            idempotency_key: None,
            group: None,
            after_group: None,
            concurrency_key: None,
        }
    }

    #[test]
    fn test_parse_put() {
        assert_eq!(parse_command("put 0 0 10 5").unwrap(), put_cmd(0, 0, 10, 5));
    }

    #[test]
    fn test_parse_put_with_idempotency_key() {
        assert_eq!(
            parse_command("put 0 0 10 5 idp:abc123").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: Some("abc123".into()),
                group: None,
                after_group: None,
                concurrency_key: None,
            }
        );
    }

    #[test]
    fn test_parse_put_with_all_extensions() {
        assert_eq!(
            parse_command("put 0 0 10 5 idp:k1 grp:g1 aft:g0 con:c1").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: Some("k1".into()),
                group: Some("g1".into()),
                after_group: Some("g0".into()),
                concurrency_key: Some("c1".into()),
            }
        );
    }

    #[test]
    fn test_parse_put_extensions_order_independent() {
        assert_eq!(
            parse_command("put 0 0 10 5 con:c1 idp:k1").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: Some("k1".into()),
                group: None,
                after_group: None,
                concurrency_key: Some("c1".into()),
            }
        );
    }

    #[test]
    fn test_parse_put_unknown_tag_rejected() {
        assert!(parse_command("put 0 0 10 5 foo:bar").is_err());
    }

    #[test]
    fn test_parse_put_invalid_key_rejected() {
        // Leading dash is invalid
        assert!(parse_command("put 0 0 10 5 idp:-bad").is_err());
        // Empty value
        assert!(parse_command("put 0 0 10 5 idp:").is_err());
        // Space in value (would be split into separate parts, unknown tag)
        assert!(parse_command("put 0 0 10 5 idp:has space").is_err());
    }

    #[test]
    fn test_parse_reserve() {
        assert_eq!(parse_command("reserve").unwrap(), Command::Reserve);
    }

    #[test]
    fn test_parse_reserve_with_timeout() {
        assert_eq!(
            parse_command("reserve-with-timeout 30").unwrap(),
            Command::ReserveWithTimeout { timeout: 30 }
        );
    }

    #[test]
    fn test_parse_reserve_job() {
        assert_eq!(
            parse_command("reserve-job 123").unwrap(),
            Command::ReserveJob { id: 123 }
        );
    }

    #[test]
    fn test_parse_delete() {
        assert_eq!(
            parse_command("delete 123").unwrap(),
            Command::Delete { id: 123 }
        );
    }

    #[test]
    fn test_parse_release() {
        assert_eq!(
            parse_command("release 123 0 0").unwrap(),
            Command::Release {
                id: 123,
                pri: 0,
                delay: 0
            }
        );
    }

    #[test]
    fn test_parse_bury() {
        assert_eq!(
            parse_command("bury 123 0").unwrap(),
            Command::Bury { id: 123, pri: 0 }
        );
    }

    #[test]
    fn test_parse_touch() {
        assert_eq!(
            parse_command("touch 123").unwrap(),
            Command::Touch { id: 123 }
        );
    }

    #[test]
    fn test_parse_watch() {
        assert_eq!(
            parse_command("watch foo").unwrap(),
            Command::Watch {
                tube: "foo".into(),
                weight: 1
            }
        );
    }

    #[test]
    fn test_parse_watch_with_weight() {
        assert_eq!(
            parse_command("watch foo 5").unwrap(),
            Command::Watch {
                tube: "foo".into(),
                weight: 5
            }
        );
    }

    #[test]
    fn test_parse_watch_weight_zero_rejected() {
        assert!(parse_command("watch foo 0").is_err());
    }

    #[test]
    fn test_parse_watch_weight_too_large() {
        assert!(parse_command("watch foo 10000").is_err());
    }

    #[test]
    fn test_parse_ignore() {
        assert_eq!(
            parse_command("ignore foo").unwrap(),
            Command::Ignore { tube: "foo".into() }
        );
    }

    #[test]
    fn test_parse_use() {
        assert_eq!(
            parse_command("use foo").unwrap(),
            Command::Use { tube: "foo".into() }
        );
    }

    #[test]
    fn test_parse_kick() {
        assert_eq!(
            parse_command("kick 10").unwrap(),
            Command::Kick { bound: 10 }
        );
    }

    #[test]
    fn test_parse_kick_job() {
        assert_eq!(
            parse_command("kick-job 123").unwrap(),
            Command::KickJob { id: 123 }
        );
    }

    #[test]
    fn test_parse_stats() {
        assert_eq!(parse_command("stats").unwrap(), Command::Stats);
    }

    #[test]
    fn test_parse_stats_job() {
        assert_eq!(
            parse_command("stats-job 123").unwrap(),
            Command::StatsJob { id: 123 }
        );
    }

    #[test]
    fn test_parse_stats_tube() {
        assert_eq!(
            parse_command("stats-tube foo").unwrap(),
            Command::StatsTube { tube: "foo".into() }
        );
    }

    #[test]
    fn test_parse_list_tubes() {
        assert_eq!(parse_command("list-tubes").unwrap(), Command::ListTubes);
    }

    #[test]
    fn test_parse_list_tube_used() {
        assert_eq!(
            parse_command("list-tube-used").unwrap(),
            Command::ListTubeUsed
        );
    }

    #[test]
    fn test_parse_list_tubes_watched() {
        assert_eq!(
            parse_command("list-tubes-watched").unwrap(),
            Command::ListTubesWatched
        );
    }

    #[test]
    fn test_parse_pause_tube() {
        assert_eq!(
            parse_command("pause-tube foo 60").unwrap(),
            Command::PauseTube {
                tube: "foo".into(),
                delay: 60
            }
        );
    }

    #[test]
    fn test_parse_reserve_mode() {
        assert_eq!(
            parse_command("reserve-mode weighted").unwrap(),
            Command::ReserveMode {
                mode: "weighted".into()
            }
        );
    }

    #[test]
    fn test_parse_flush_tube() {
        assert_eq!(
            parse_command("flush-tube foo").unwrap(),
            Command::FlushTube { tube: "foo".into() }
        );
    }

    #[test]
    fn test_parse_flush_tube_bad_name() {
        assert!(parse_command("flush-tube -bad").is_err());
        assert!(parse_command("flush-tube").is_err());
    }

    #[test]
    fn test_parse_quit() {
        assert_eq!(parse_command("quit").unwrap(), Command::Quit);
    }

    #[test]
    fn test_parse_unknown() {
        assert!(matches!(
            parse_command("foobar").unwrap_err(),
            Response::UnknownCommand
        ));
    }

    #[test]
    fn test_parse_bad_format() {
        assert!(parse_command("put abc").is_err());
        assert!(parse_command("delete").is_err());
        assert!(parse_command("release 1 2").is_err());
    }

    #[test]
    fn test_tube_name_validation() {
        assert!(is_valid_tube_name("default"));
        assert!(is_valid_tube_name("my_tube.v2"));
        assert!(is_valid_tube_name("a"));
        assert!(!is_valid_tube_name(""));
        assert!(!is_valid_tube_name("-bad"));
        assert!(!is_valid_tube_name("has space"));
        assert!(!is_valid_tube_name("has\ttab"));
    }

    #[test]
    fn test_tube_name_max_length() {
        let name_200 = "a".repeat(200);
        assert!(is_valid_tube_name(&name_200));

        let name_201 = "a".repeat(201);
        assert!(!is_valid_tube_name(&name_201));
    }

    #[test]
    fn test_response_serialize() {
        assert_eq!(Response::Deleted.serialize(), b"DELETED\r\n");
        assert_eq!(Response::Inserted(42).serialize(), b"INSERTED 42\r\n");
        assert_eq!(Response::NotFound.serialize(), b"NOT_FOUND\r\n");
    }
}
