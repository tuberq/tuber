use super::*;

fn make_state() -> ServerState {
    ServerState::new(65535, None, None, None, Arc::new(FdBudget::new(Some(0))))
}

fn register(state: &mut ServerState) -> u64 {
    static NEXT_CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let id = NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    state.conns.insert(id, ConnState::new(id));
    state.stats.total_connections += 1;
    state.ensure_tube("default");
    if let Some(t) = state.tubes.get_mut("default") {
        t.watching_ct += 1;
        t.using_ct += 1;
    }
    id
}

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

fn put_con_cmd(pri: u32, ttr: u32, bytes: u32, key: &str, limit: u32) -> Command {
    Command::Put {
        pri,
        delay: 0,
        ttr,
        bytes,
        idempotency_key: None,
        group: None,
        after_group: None,
        concurrency_key: Some((key.to_string(), limit)),
    }
}

#[test]
fn test_put_and_reserve() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(c, put_cmd(0, 0, 10, 5), Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    let resp = s.handle_command(c, Command::Reserve, None);
    match resp {
        Response::Reserved { id, body } => {
            assert_eq!(id, 1);
            assert_eq!(body, b"hello");
        }
        _ => panic!("expected Reserved, got {:?}", resp),
    }
}

#[test]
fn test_priority_ordering() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(5, 0, 10, 1), Some(b"a".to_vec()));
    s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"b".to_vec()));
    s.handle_command(c, put_cmd(3, 0, 10, 1), Some(b"c".to_vec()));

    // Should get priority 1 first
    let resp = s.handle_command(c, Command::Reserve, None);
    match resp {
        Response::Reserved { body, .. } => assert_eq!(body, b"b"),
        _ => panic!("expected Reserved"),
    }
}

#[test]
fn test_fifo_same_priority() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"a".to_vec()));
    s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"b".to_vec()));
    s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"c".to_vec()));

    let resp = s.handle_command(c, Command::Reserve, None);
    match resp {
        Response::Reserved { body, .. } => assert_eq!(body, b"a"),
        _ => panic!("expected Reserved"),
    }
    let resp = s.handle_command(c, Command::Reserve, None);
    match resp {
        Response::Reserved { body, .. } => assert_eq!(body, b"b"),
        _ => panic!("expected Reserved"),
    }
}

#[test]
fn test_delete_ready() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    let resp = s.handle_command(c, Command::Peek { id: 1 }, None);
    assert!(matches!(resp, Response::NotFound));
}

#[test]
fn test_bury_and_kick() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    let resp = s.handle_command(c, Command::Reserve, None);
    let id = match resp {
        Response::Reserved { id, .. } => id,
        _ => panic!("expected Reserved"),
    };

    let resp = s.handle_command(c, Command::Bury { id, pri: 0 }, None);
    assert!(matches!(resp, Response::Buried));

    let resp = s.handle_command(c, Command::Kick { bound: 1 }, None);
    assert!(matches!(resp, Response::Kicked(1)));

    // Job should be ready again
    let resp = s.handle_command(c, Command::PeekReady, None);
    assert!(matches!(resp, Response::Found { .. }));
}

#[test]
fn test_multi_tube() {
    let mut s = make_state();
    let producer = register(&mut s);
    let consumer = register(&mut s);

    // Producer uses "emails"
    s.handle_command(
        producer,
        Command::Use {
            tube: "emails".into(),
        },
        None,
    );
    s.handle_command(producer, put_cmd(0, 0, 10, 5), Some(b"hello".to_vec()));

    // Consumer watches "emails", reserve should find it
    s.handle_command(
        consumer,
        Command::Watch {
            tube: "emails".into(),
            weight: 1,
        },
        None,
    );
    let resp = s.handle_command(consumer, Command::Reserve, None);
    match resp {
        Response::Reserved { body, .. } => assert_eq!(body, b"hello"),
        _ => panic!("expected Reserved, got {:?}", resp),
    }
}

#[test]
fn test_release_to_ready() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(5, 0, 10, 1), Some(b"x".to_vec()));
    let resp = s.handle_command(c, Command::Reserve, None);
    let id = match resp {
        Response::Reserved { id, .. } => id,
        _ => panic!(),
    };

    let resp = s.handle_command(
        c,
        Command::Release {
            id,
            pri: 0,
            delay: 0,
        },
        None,
    );
    assert!(matches!(resp, Response::Released));

    // Should be back in ready with new priority
    let resp = s.handle_command(c, Command::PeekReady, None);
    assert!(matches!(resp, Response::Found { .. }));
}

#[test]
fn test_touch() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    let resp = s.handle_command(c, Command::Reserve, None);
    let id = match resp {
        Response::Reserved { id, .. } => id,
        _ => panic!(),
    };

    let resp = s.handle_command(c, Command::Touch { id }, None);
    assert!(matches!(resp, Response::Touched));
}

#[test]
fn test_drain_mode() {
    let mut s = make_state();
    let c = register(&mut s);
    s.drain_mode = true;

    let resp = s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    assert!(matches!(resp, Response::Draining));
}

#[test]
fn test_undrain_mode() {
    let mut s = make_state();
    let c = register(&mut s);

    // Enter drain mode
    let resp = s.handle_command(c, Command::Drain, None);
    assert!(matches!(resp, Response::Draining));

    // Put should fail
    let resp = s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    assert!(matches!(resp, Response::Draining));

    // Undrain
    let resp = s.handle_command(c, Command::Undrain, None);
    assert!(matches!(resp, Response::NotDraining));

    // Put should work again
    let resp = s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    assert!(matches!(resp, Response::Inserted(_)));
}

#[test]
fn test_close_releases_jobs() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    s.handle_command(c, Command::Reserve, None);

    // Disconnect
    s.unregister_conn(c);

    // Job should be back in ready (for another consumer)
    assert_eq!(s.ready_ct, 1);
}

#[test]
fn test_reserve_mode_weighted() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(
        c,
        Command::ReserveMode {
            mode: "weighted".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Using(m) if m == "weighted"));

    let resp = s.handle_command(
        c,
        Command::ReserveMode {
            mode: "fifo".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Using(m) if m == "fifo"));

    let resp = s.handle_command(
        c,
        Command::ReserveMode {
            mode: "invalid".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::BadFormat));
}

#[test]
fn test_reserve_mode_weighted_fair() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(
        c,
        Command::ReserveMode {
            mode: "weighted-fair".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Using(m) if m == "weighted-fair"));

    // Switch back to fifo
    let resp = s.handle_command(
        c,
        Command::ReserveMode {
            mode: "fifo".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Using(m) if m == "fifo"));
}

#[test]
fn test_weighted_fair_favors_fast_tubes() {
    let mut s = make_state();
    let c = register(&mut s);

    // Set up weighted-fair mode
    s.handle_command(
        c,
        Command::ReserveMode {
            mode: "weighted-fair".into(),
        },
        None,
    );

    // Watch "fast" tube (weight 1) and "slow" tube (weight 1)
    s.handle_command(
        c,
        Command::Watch {
            tube: "fast".into(),
            weight: 1,
        },
        None,
    );
    s.handle_command(
        c,
        Command::Watch {
            tube: "slow".into(),
            weight: 1,
        },
        None,
    );
    s.handle_command(
        c,
        Command::Ignore {
            tube: "default".into(),
        },
        None,
    );

    // Put jobs into both tubes
    s.handle_command(
        c,
        Command::Use {
            tube: "fast".into(),
        },
        None,
    );
    for _ in 0..100 {
        s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"f".to_vec()));
    }

    s.handle_command(
        c,
        Command::Use {
            tube: "slow".into(),
        },
        None,
    );
    for _ in 0..100 {
        s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"s".to_vec()));
    }

    // Set processing time EWMAs: fast=0.1s, slow=10.0s (100x slower)
    s.tubes.get_mut("fast").unwrap().stat.processing_time_ewma = 0.1;
    s.tubes
        .get_mut("fast")
        .unwrap()
        .stat
        .processing_time_samples = 100;
    s.tubes.get_mut("slow").unwrap().stat.processing_time_ewma = 10.0;
    s.tubes
        .get_mut("slow")
        .unwrap()
        .stat
        .processing_time_samples = 100;

    // Reserve many jobs and count which tube they came from
    let mut fast_count = 0u32;
    let mut slow_count = 0u32;
    for _ in 0..50 {
        let resp = s.handle_command(c, Command::Reserve, None);
        if let Response::Reserved { id, .. } = resp {
            let job = s.jobs.get(&id).unwrap();
            if job.tube_name.as_ref() == "fast" {
                fast_count += 1;
            } else {
                slow_count += 1;
            }
            // Delete so the job doesn't stay reserved
            s.handle_command(c, Command::Delete { id }, None);
        }
    }

    // With equal weights and 100x processing time difference,
    // fast tube should get ~99% of reserves.
    // Be generous: fast should get at least 80% (40 out of 50).
    assert!(
        fast_count > 40,
        "weighted-fair should heavily favor fast tube: fast={fast_count}, slow={slow_count}"
    );
}

#[test]
fn test_weighted_fair_fallback_no_ewma() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(
        c,
        Command::ReserveMode {
            mode: "weighted-fair".into(),
        },
        None,
    );

    // Watch two tubes, neither has processing time data
    s.handle_command(
        c,
        Command::Watch {
            tube: "a".into(),
            weight: 3,
        },
        None,
    );
    s.handle_command(
        c,
        Command::Watch {
            tube: "b".into(),
            weight: 1,
        },
        None,
    );
    s.handle_command(
        c,
        Command::Ignore {
            tube: "default".into(),
        },
        None,
    );

    // Put one job in each
    s.handle_command(c, Command::Use { tube: "a".into() }, None);
    s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"a".to_vec()));
    s.handle_command(c, Command::Use { tube: "b".into() }, None);
    s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"b".to_vec()));

    // Both tubes have ewma=0 so effective weights should fall back to raw weights.
    // Just verify we can reserve both without panicking.
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(matches!(resp, Response::Reserved { .. }));
}

#[test]
fn test_watch_and_ignore() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(
        c,
        Command::Watch {
            tube: "foo".into(),
            weight: 1,
        },
        None,
    );
    assert!(matches!(resp, Response::Watching(2)));

    let resp = s.handle_command(
        c,
        Command::Ignore {
            tube: "default".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Watching(1)));

    // Can't ignore last tube
    let resp = s.handle_command(c, Command::Ignore { tube: "foo".into() }, None);
    assert!(matches!(resp, Response::NotIgnored));
}

#[test]
fn test_kick_job_buried() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None);

    let resp = s.handle_command(c, Command::KickJob { id: 1 }, None);
    assert!(matches!(resp, Response::KickedOne));
}

#[test]
fn test_pause_tube() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(
        c,
        Command::PauseTube {
            tube: "default".into(),
            delay: 60,
        },
        None,
    );
    assert!(matches!(resp, Response::Paused));

    assert!(s.tubes.get("default").unwrap().is_paused());
}

#[test]
fn test_pause_tube_zero_unpauses_immediately() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 120, 1), Some(b"a".to_vec()));

    s.handle_command(
        c,
        Command::PauseTube {
            tube: "default".into(),
            delay: 60,
        },
        None,
    );
    assert!(s.tubes.get("default").unwrap().is_paused());

    // `pause-tube <t> 0` means resume now, not "pause for one second".
    let resp = s.handle_command(
        c,
        Command::PauseTube {
            tube: "default".into(),
            delay: 0,
        },
        None,
    );
    assert!(matches!(resp, Response::Paused));

    let tube = s.tubes.get("default").unwrap();
    assert!(!tube.is_paused());
    assert_eq!(tube.pause, Duration::ZERO, "stats-tube pause must read 0");
    assert!(tube.unpause_at.is_none());

    // And the job is reservable straight away, with no wait for the deadline.
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(
        matches!(resp, Response::Reserved { .. }),
        "expected immediate RESERVED, got {resp:?}"
    );
}

#[test]
fn test_stats_job() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(100, 0, 60, 5), Some(b"hello".to_vec()));

    let resp = s.handle_command(c, Command::StatsJob { id: 1 }, None);
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("id: 1"));
            assert!(yaml.contains("state: ready"));
            assert!(yaml.contains("pri: 100"));
        }
        _ => panic!("expected Ok"),
    }
}

#[test]
fn test_stats_job_with_extensions() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: Some("grp1".into()),
        after_group: Some("grp0".into()),
        concurrency_key: Some(("con1".into(), 1)),
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));

    let resp = s.handle_command(c, Command::StatsJob { id: 1 }, None);
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("idempotency-key: mykey"), "yaml: {}", yaml);
            assert!(yaml.contains("group: grp1"), "yaml: {}", yaml);
            assert!(yaml.contains("after-group: grp0"), "yaml: {}", yaml);
            assert!(yaml.contains("concurrency-key: con1"), "yaml: {}", yaml);
            assert!(yaml.contains("concurrency-limit: 1"), "yaml: {}", yaml);
        }
        _ => panic!("expected Ok"),
    }
}

#[test]
fn test_list_tubes() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(
        c,
        Command::Use {
            tube: "emails".into(),
        },
        None,
    );

    let resp = s.handle_command(c, Command::ListTubes, None);
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("- default"));
            assert!(yaml.contains("- emails"));
        }
        _ => panic!("expected Ok"),
    }
}

#[test]
fn test_concurrency_key_limit_n() {
    // Put 3 jobs with con:api:2, reserve 2 (succeed), 3rd blocked, delete 1, 3rd succeeds
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);

    let con_put = |s: &mut ServerState, conn: u64, body: &[u8]| {
        s.handle_command(
            conn,
            put_con_cmd(0, 120, body.len() as u32, "api", 2),
            Some(body.to_vec()),
        )
    };

    // Insert 3 jobs
    assert!(matches!(con_put(&mut s, c1, b"j1"), Response::Inserted(1)));
    assert!(matches!(con_put(&mut s, c1, b"j2"), Response::Inserted(2)));
    assert!(matches!(con_put(&mut s, c1, b"j3"), Response::Inserted(3)));

    // Reserve first two — should succeed
    let r1 = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r1, Response::Reserved { id: 1, .. }));

    let r2 = s.handle_command(c2, Command::Reserve, None);
    assert!(matches!(r2, Response::Reserved { id: 2, .. }));

    // Third reserve should be blocked (2 already reserved, limit is 2)
    let r3 = s.handle_command(c1, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(matches!(r3, Response::TimedOut));

    // Delete one reserved job — frees a slot
    let resp = s.handle_command(c1, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    // Now reserve should succeed
    let r4 = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r4, Response::Reserved { id: 3, .. }));
}

#[test]
fn test_concurrency_key_default_limit_one() {
    // con:key (no :N) defaults to limit 1
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);

    let con_put = |s: &mut ServerState, conn: u64, body: &[u8]| {
        s.handle_command(
            conn,
            put_con_cmd(0, 120, body.len() as u32, "single", 1),
            Some(body.to_vec()),
        )
    };

    con_put(&mut s, c1, b"j1");
    con_put(&mut s, c1, b"j2");

    // Reserve first — succeeds
    let r1 = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r1, Response::Reserved { id: 1, .. }));

    // Second reserve should be blocked
    let r2 = s.handle_command(c2, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(matches!(r2, Response::TimedOut));
}

#[test]
fn a_con_key_takes_the_strictest_limit_of_its_jobs() {
    let mut s = make_state();
    let (c1, c2) = (register(&mut s), register(&mut s));
    let put = |s: &mut ServerState, pri: u32, limit: u32| {
        s.handle_command(
            c1,
            put_con_cmd(pri, 120, 1, "k", limit),
            Some(b"x".to_vec()),
        )
    };
    assert_eq!(put(&mut s, 0, 3), Response::Inserted(1));
    assert_eq!(put(&mut s, 10, 1), Response::Inserted(2));
    assert_eq!(put(&mut s, 0, 3), Response::Inserted(3));

    // Job 2 asked for one at a time, so nothing runs beside job 1, however
    // job 1 was declared or which was reserved first.
    assert!(matches!(
        s.handle_command(c1, Command::Reserve, None),
        Response::Reserved { id: 1, .. }
    ));
    assert_eq!(
        s.handle_command(c2, Command::ReserveWithTimeout { timeout: 0 }, None),
        Response::TimedOut
    );

    // Once job 2 is gone, the key's limit is 3.
    assert_eq!(
        s.handle_command(c1, Command::Delete { id: 2 }, None),
        Response::Deleted
    );
    assert!(matches!(
        s.handle_command(c2, Command::Reserve, None),
        Response::Reserved { id: 3, .. }
    ));
}

#[test]
fn a_buried_job_does_not_hold_its_con_key_to_its_limit() {
    let mut s = make_state();
    let (c1, c2) = (register(&mut s), register(&mut s));
    let put = |s: &mut ServerState, limit: u32| {
        s.handle_command(c1, put_con_cmd(0, 120, 1, "k", limit), Some(b"x".to_vec()))
    };
    put(&mut s, 1);
    put(&mut s, 3);
    put(&mut s, 3);
    assert!(matches!(
        s.handle_command(c1, Command::Reserve, None),
        Response::Reserved { id: 1, .. }
    ));
    assert_eq!(
        s.handle_command(c1, Command::Bury { id: 1, pri: 0 }, None),
        Response::Buried
    );

    // Buried, job 1 doesn't count: an old job left buried after a deploy
    // raised the limit would otherwise hold it down indefinitely.
    for (conn, id) in [(c1, 2), (c2, 3)] {
        assert!(matches!(
            s.handle_command(conn, Command::Reserve, None),
            Response::Reserved { id: got, .. } if got == id
        ));
    }
    // Kicked, it counts again: with two running it has to wait.
    assert_eq!(
        s.handle_command(c1, Command::Kick { bound: 1 }, None),
        Response::Kicked(1)
    );
    let c3 = register(&mut s);
    assert_eq!(
        s.handle_command(c3, Command::ReserveWithTimeout { timeout: 0 }, None),
        Response::TimedOut
    );
}

/// Bring delayed job `id`'s deadline forward to now, so the next tick
/// promotes it.
fn make_due(s: &mut ServerState, id: u64) {
    let now = Instant::now();
    let job = s.jobs.get_mut(&id).unwrap();
    job.deadline_at = Some(now);
    let delay = &mut s.tubes.get_mut(&*job.tube_name).unwrap().delay;
    delay.remove_by_id(id);
    delay.insert((now, id), id);
}

#[test]
fn a_delayed_job_does_not_hold_its_con_key_to_its_limit() {
    let mut s = make_state();
    let (c1, c2, c3) = (register(&mut s), register(&mut s), register(&mut s));
    let delayed = Command::Put {
        pri: 10,
        delay: 3600,
        ttr: 120,
        bytes: 1,
        idempotency_key: None,
        group: None,
        after_group: None,
        concurrency_key: Some(("k".to_string(), 1)),
    };
    assert_eq!(
        s.handle_command(c1, delayed, Some(b"x".to_vec())),
        Response::Inserted(1)
    );
    for id in [2, 3, 4] {
        assert_eq!(
            s.handle_command(c1, put_con_cmd(0, 120, 1, "k", 3), Some(b"x".to_vec())),
            Response::Inserted(id)
        );
    }

    // Job 1 asks for one at a time, but isn't due for an hour. Until then it
    // doesn't hold the others to its limit.
    for (conn, id) in [(c1, 2), (c2, 3)] {
        assert!(matches!(
            s.handle_command(conn, Command::Reserve, None),
            Response::Reserved { id: got, .. } if got == id
        ));
    }
    // Due, it counts: with two running, neither it nor job 4 can start.
    make_due(&mut s, 1);
    s.tick();
    assert_eq!(s.jobs[&1].state, JobState::Ready);
    assert_eq!(
        s.handle_command(c3, Command::ReserveWithTimeout { timeout: 0 }, None),
        Response::TimedOut
    );
}

#[test]
fn test_cmd_delete_wakes_concurrency_waiter() {
    // A reserver parked while a con: slot is held must be fulfilled
    // synchronously when the holder deletes the job. Without process_queue
    // in cmd_delete, the waiter would only wake on the next tick.
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);

    let con_put = |s: &mut ServerState, conn: u64, body: &[u8]| {
        s.handle_command(
            conn,
            put_con_cmd(0, 120, body.len() as u32, "k", 1),
            Some(body.to_vec()),
        )
    };

    con_put(&mut s, c1, b"j1");
    con_put(&mut s, c1, b"j2");

    // c1 reserves j1, taking the only con-key slot.
    let r1 = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r1, Response::Reserved { id: 1, .. }));

    // c2 parks as a waiter (j2 is con-blocked by j1's slot).
    let (tx, mut rx) = oneshot::channel();
    s.add_waiter(c2, tx, None, None);
    assert_eq!(s.stats.waiting_ct, 1);

    // c1 deletes j1 — slot freed, waiter must be fulfilled with j2.
    let resp = s.handle_command(c1, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    assert_eq!(
        s.stats.waiting_ct, 0,
        "delete must wake parked waiter when concurrency slot is freed"
    );
    let received = rx
        .try_recv()
        .expect("waiter should have received a response");
    assert!(
        matches!(received, Response::Reserved { id: 2, .. }),
        "expected Reserved(2), got {:?}",
        received
    );
}

#[test]
fn flush_tube_wakes_a_worker_waiting_on_the_con_keys_it_frees() {
    // Like delete: a flushed reserved job frees its slot, and a flushed
    // ready one can loosen the key's strictest limit. Either can unblock a
    // waiter in another tube, which shouldn't have to wait for the tick.
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);
    let c3 = register(&mut s);

    let other = || Command::Use {
        tube: "other".into(),
    };
    s.handle_command(c1, other(), None);
    s.handle_command(
        c1,
        Command::Watch {
            tube: "other".into(),
            weight: 1,
        },
        None,
    );
    s.handle_command(
        c1,
        Command::Ignore {
            tube: "default".into(),
        },
        None,
    );
    s.handle_command(c1, put_con_cmd(0, 120, 1, "k", 2), Some(b"a".to_vec()));
    s.handle_command(c1, put_con_cmd(0, 120, 1, "k", 1), Some(b"b".to_vec()));
    s.handle_command(c2, put_con_cmd(0, 120, 1, "k", 2), Some(b"c".to_vec()));
    // Job 1 takes a slot, and job 2 holds the key to one.
    let r = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r, Response::Reserved { id: 1, .. }), "got {r:?}");

    let (tx, mut rx) = oneshot::channel();
    s.add_waiter(c2, tx, None, None);
    assert_eq!(s.stats.waiting_ct, 1, "job 3 is blocked by the key");

    let resp = s.handle_command(
        c3,
        Command::FlushTube {
            tube: "other".into(),
        },
        None,
    );
    assert_eq!(resp, Response::Flushed(2));
    let received = rx.try_recv().expect("the flush should wake the waiter");
    assert!(
        matches!(received, Response::Reserved { id: 3, .. }),
        "got {received:?}"
    );
}

#[test]
fn test_find_unblocked_returns_smallest_unblocked() {
    // Correctness probe for find_best_unblocked_ready: with the heap top
    // blocked by a con: key, the function must still return the
    // lowest-priority unblocked job, not just any unblocked one.
    let mut s = make_state();
    let c1 = register(&mut s);
    let _c2 = register(&mut s);

    // j1: pri 1 con:k (blocks); j2: pri 2 unblocked (target);
    // j3: pri 0 con:k (smallest overall but blocked).
    let send = |s: &mut ServerState, cmd: Command, body: &[u8]| {
        s.handle_command(c1, cmd, Some(body.to_vec()))
    };
    assert!(matches!(send(&mut s, put_con_cmd(1, 120, 1, "k", 1), b"a"), Response::Inserted(1)));
    assert!(matches!(send(&mut s, put_cmd(2, 0, 120, 1),          b"b"), Response::Inserted(2)));
    assert!(matches!(send(&mut s, put_con_cmd(0, 120, 1, "k", 1), b"c"), Response::Inserted(3)));

    // Reserve j3 (pri 0) — takes the con:k slot.
    let r = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r, Response::Reserved { id: 3, .. }));

    // Now j1 (pri 1, con:k) is at heap top but blocked.
    // The next unblocked is j2 (pri 2, no con). We must NOT return
    // anything else, even if a future change adds out-of-order traversal.
    let r = s.handle_command(c1, Command::Reserve, None);
    assert!(
        matches!(r, Response::Reserved { id: 2, .. }),
        "expected smallest-unblocked j2, got {:?}",
        r
    );
}

/// `reserve-with-timeout 0`: the reserved id, or None on TIMED_OUT.
fn reserve_now(s: &mut ServerState, conn: u64) -> Option<u64> {
    match s.handle_command(conn, Command::ReserveWithTimeout { timeout: 0 }, None) {
        Response::Reserved { id, .. } => Some(id),
        Response::TimedOut => None,
        other => panic!("unexpected reserve response {other:?}"),
    }
}

#[test]
fn test_deep_con_backlog_does_not_hide_untagged_jobs() {
    // 300 con:A jobs ahead of 5 untagged ones at the same priority. Once A is
    // held, the untagged jobs are next; a search that gives up partway down
    // the blocked backlog reported the tube empty instead.
    let mut s = make_state();
    let c = register(&mut s);
    for _ in 0..300 {
        s.handle_command(c, put_con_cmd(0, 120, 1, "A", 1), Some(b"a".to_vec()));
    }
    for _ in 0..5 {
        s.handle_command(c, put_cmd(0, 0, 120, 1), Some(b"b".to_vec()));
    }

    assert_eq!(reserve_now(&mut s, c), Some(1));
    for want in 301..=305 {
        assert_eq!(reserve_now(&mut s, c), Some(want));
    }
    assert_eq!(
        reserve_now(&mut s, c),
        None,
        "only blocked con:A jobs are left"
    );
}

#[test]
fn test_many_blocked_con_keys_do_not_hide_ready_jobs() {
    // 300 keys with two jobs each, then one untagged job. Each reserve takes
    // a key's first job and leaves its second blocked, so by the end 300
    // blocked jobs sort ahead of every job still reservable.
    let mut s = make_state();
    let c = register(&mut s);
    for k in 0..300 {
        for _ in 0..2 {
            let key = format!("k{k}");
            s.handle_command(c, put_con_cmd(0, 120, 1, &key, 1), Some(b"a".to_vec()));
        }
    }
    s.handle_command(c, put_cmd(0, 0, 120, 1), Some(b"b".to_vec()));

    for k in 0..300 {
        assert_eq!(reserve_now(&mut s, c), Some(2 * k + 1), "first job of k{k}");
    }
    assert_eq!(reserve_now(&mut s, c), Some(601), "the untagged job");
    assert_eq!(reserve_now(&mut s, c), None);
}

#[test]
fn test_parked_worker_gets_job_put_behind_deep_con_backlog() {
    // The waiter path: a put behind a blocked backlog must wake a parked
    // worker straight away, not leave it parked with a job ready.
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);
    for _ in 0..300 {
        s.handle_command(c1, put_con_cmd(0, 120, 1, "A", 1), Some(b"a".to_vec()));
    }
    assert_eq!(reserve_now(&mut s, c1), Some(1));

    let (tx, mut rx) = oneshot::channel();
    s.add_waiter(c2, tx, None, None);
    assert!(matches!(
        s.handle_command(c1, put_cmd(0, 0, 120, 1), Some(b"b".to_vec())),
        Response::Inserted(301)
    ));

    let received = rx
        .try_recv()
        .expect("the parked worker should have been woken");
    assert!(
        matches!(received, Response::Reserved { id: 301, .. }),
        "expected Reserved(301), got {received:?}"
    );
}

/// The best reservable job in `tube` by exhaustive search: the smallest
/// (pri, id) among its ready jobs whose `con:` key has a free slot.
fn brute_force_best_unblocked(s: &ServerState, tube: &str) -> Option<u64> {
    s.jobs
        .values()
        .filter(|j| j.state == JobState::Ready && &*j.tube_name == tube)
        .filter(|j| !s.is_concurrency_blocked(j.id))
        .map(|j| j.ready_key())
        .min()
        .map(|(_, id)| id)
}

#[test]
fn test_find_best_unblocked_matches_brute_force_under_random_ops() {
    // Random puts, reserves, deletes, releases, buries, kicks, delays coming
    // due, idp: priority upgrades, aft: promotions, disconnects and flushes
    // across two tubes sharing con: keys. After every step, each tube's pick
    // must equal an exhaustive search, and its ready count must match the
    // jobs in Ready.
    const TUBES: [&str; 2] = ["default", "other"];
    const KEYS: [&str; 4] = ["a", "b", "c", "d"];

    let mut blocked_tops = 0;
    for seed in 1..=8u64 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut next = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };

        let mut s = make_state();
        let mut conns: Vec<u64> = Vec::new();
        let new_conn = |s: &mut ServerState| {
            let c = register(s);
            s.handle_command(
                c,
                Command::Watch {
                    tube: "other".into(),
                    weight: 1,
                },
                None,
            );
            c
        };
        for _ in 0..4 {
            conns.push(new_conn(&mut s));
        }

        for step in 0..2500 {
            let c = conns[next(conns.len() as u64) as usize];
            let max_id = s.next_job_id.max(1);
            let held: Vec<u64> = s
                .conns
                .get(&c)
                .map(|cs| cs.reserved_jobs.clone())
                .unwrap_or_default();
            let any_held = |next: &mut dyn FnMut(u64) -> u64| {
                (!held.is_empty()).then(|| held[next(held.len() as u64) as usize])
            };
            match next(100) {
                0..=34 => {
                    let tube = TUBES[next(2) as usize];
                    s.handle_command(c, Command::Use { tube: tube.into() }, None);
                    let concurrency_key = (next(10) < 7)
                        .then(|| (KEYS[next(4) as usize].to_string(), 1 + next(3) as u32));
                    let idempotency_key = (next(10) == 0).then(|| (format!("i{}", next(3)), 0));
                    let (group, after_group) = match next(20) {
                        0 => (Some("g".to_string()), None),
                        1 => (None, Some("g".to_string())),
                        _ => (None, None),
                    };
                    let cmd = Command::Put {
                        pri: next(4) as u32,
                        delay: if next(10) == 0 { 60 } else { 0 },
                        ttr: 120,
                        bytes: 1,
                        idempotency_key,
                        group,
                        after_group,
                        concurrency_key,
                    };
                    s.handle_command(c, cmd, Some(b"x".to_vec()));
                }
                35..=59 => {
                    reserve_now(&mut s, c);
                }
                60..=69 => {
                    if let Some(id) = any_held(&mut next) {
                        s.handle_command(c, Command::Delete { id }, None);
                    } else {
                        s.handle_command(
                            c,
                            Command::Delete {
                                id: 1 + next(max_id),
                            },
                            None,
                        );
                    }
                }
                70..=79 => {
                    if let Some(id) = any_held(&mut next) {
                        let pri = next(4) as u32;
                        let delay = if next(4) == 0 { 60 } else { 0 };
                        s.handle_command(c, Command::Release { id, pri, delay }, None);
                    }
                }
                80..=84 => {
                    if let Some(id) = any_held(&mut next) {
                        s.handle_command(
                            c,
                            Command::Bury {
                                id,
                                pri: next(4) as u32,
                            },
                            None,
                        );
                    }
                }
                85..=87 => {
                    let tube = TUBES[next(2) as usize];
                    s.handle_command(c, Command::Use { tube: tube.into() }, None);
                    s.handle_command(
                        c,
                        Command::Kick {
                            bound: 1 + next(3) as u32,
                        },
                        None,
                    );
                }
                88..=89 => {
                    // Let a delayed job come due.
                    let mut delayed: Vec<u64> = s
                        .jobs
                        .values()
                        .filter(|j| j.state == JobState::Delayed && j.deadline_at.is_some())
                        .map(|j| j.id)
                        .collect();
                    delayed.sort_unstable();
                    if !delayed.is_empty() {
                        make_due(&mut s, delayed[next(delayed.len() as u64) as usize]);
                    }
                    s.tick();
                }
                90..=93 => {
                    // A random id is seldom a buried or delayed job; aim for one.
                    let mut parked: Vec<u64> = s
                        .jobs
                        .values()
                        .filter(|j| matches!(j.state, JobState::Buried | JobState::Delayed))
                        .map(|j| j.id)
                        .collect();
                    parked.sort_unstable();
                    let id = match next(2) {
                        0 if !parked.is_empty() => parked[next(parked.len() as u64) as usize],
                        _ => 1 + next(max_id),
                    };
                    let cmd = match next(3) {
                        0 => Command::KickJob { id },
                        1 => Command::ReserveJob { id },
                        _ => Command::Delete { id },
                    };
                    s.handle_command(c, cmd, None);
                }
                94..=97 => {
                    s.unregister_conn(c);
                    conns.retain(|&x| x != c);
                    conns.push(new_conn(&mut s));
                }
                _ => {
                    let tube = TUBES[next(2) as usize];
                    s.handle_command(c, Command::FlushTube { tube: tube.into() }, None);
                }
            }

            for name in TUBES {
                let Some(tube) = s.tubes.get(name) else {
                    continue;
                };
                if tube
                    .ready
                    .peek()
                    .is_some_and(|e| s.is_concurrency_blocked(e.1))
                {
                    blocked_tops += 1;
                }
                let got = s.find_best_unblocked_ready(tube).map(|(_, id)| id);
                let want = brute_force_best_unblocked(&s, name);
                assert_eq!(got, want, "seed {seed} step {step} tube {name}: pick");
                let ready = s
                    .jobs
                    .values()
                    .filter(|j| j.state == JobState::Ready && &*j.tube_name == name)
                    .count();
                assert_eq!(
                    tube.ready.len(),
                    ready,
                    "seed {seed} step {step} tube {name}: count"
                );
                let held = s
                    .jobs
                    .values()
                    .filter(|j| {
                        j.state == JobState::Delayed
                            && j.deadline_at.is_none()
                            && &*j.tube_name == name
                    })
                    .count();
                assert_eq!(
                    tube.stat.held_ct, held as u64,
                    "seed {seed} step {step} tube {name}: held"
                );
                tube.ready.assert_consistent();
            }
            // Each key's limits are those its ready and reserved jobs declare.
            let mut declared: HashMap<String, BTreeMap<u32, u32>> = HashMap::new();
            for j in s
                .jobs
                .values()
                .filter(|j| matches!(j.state, JobState::Ready | JobState::Reserved))
            {
                if let Some((key, limit)) = j.concurrency_key() {
                    *declared
                        .entry(key.clone())
                        .or_default()
                        .entry(*limit)
                        .or_default() += 1;
                }
            }
            assert_eq!(
                s.concurrency_limits, declared,
                "seed {seed} step {step}: con limits"
            );
        }
    }
    eprintln!("steps with a blocked top: {blocked_tops}");
    assert!(
        blocked_tops > 1000,
        "only {blocked_tops} steps had a blocked top job to skip"
    );
}

#[test]
fn test_idempotency_ttl_cooldown() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put with idp:key:5
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 5)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Reserve and delete
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(matches!(resp, Response::Reserved { id: 1, .. }));
    let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    // Re-put with same key should return original ID with DELETED state (cooldown active)
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 5)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "DELETED", None)));
}

#[test]
fn test_idempotency_no_ttl_no_cooldown() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put with idp:key (no TTL)
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Reserve and delete
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(matches!(resp, Response::Reserved { id: 1, .. }));
    let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    // Re-put should get new ID (no cooldown)
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::Inserted(2)));
}

#[test]
fn test_idempotency_ttl_flush_clears_cooldowns() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put with cooldown
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 60)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Delete to start cooldown, then flush
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(matches!(resp, Response::Reserved { id: 1, .. }));
    let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    // Flush tube clears cooldowns
    let resp = s.handle_command(
        c,
        Command::FlushTube {
            tube: "default".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Flushed(0)));

    // Re-put should get new ID
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 60)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::Inserted(2)));
}

#[test]
fn test_idempotency_dedup_returns_ready_state() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Duplicate put returns InsertedDup with READY state
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "READY", None)));
}

#[test]
fn test_idempotency_dedup_returns_reserved_state() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));

    // Reserve the job
    s.handle_command(c, Command::Reserve, None);

    // Duplicate put returns InsertedDup with RESERVED state
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "RESERVED", None)));
}

#[test]
fn test_idempotency_dedup_returns_buried_state() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));

    // Reserve and bury
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None);

    // Duplicate put returns InsertedDup with BURIED state
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "BURIED", None)));
}

#[test]
fn test_idempotency_dedup_returns_delayed_state() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 3600,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));

    // Duplicate put returns InsertedDup with DELAYED state
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "DELAYED", None)));
}

#[test]
fn test_idempotency_cooldown_dedup_returns_deleted_state() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 60)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));

    // Reserve and delete (starts cooldown)
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Delete { id: 1 }, None);

    // Re-put during cooldown returns InsertedDup with DELETED state
    let cmd2 = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 60)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "DELETED", None)));
}

#[test]
fn test_idempotency_priority_upgrade_ready() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put job at pri 100
    let cmd1 = Command::Put {
        pri: 100,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Put another job at pri 75 (no IDP) to compare ordering
    let cmd2 = Command::Put {
        pri: 75,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: None,
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"other".to_vec()));
    assert!(matches!(resp, Response::Inserted(2)));

    // Duplicate put at pri 50 — should upgrade
    let cmd3 = Command::Put {
        pri: 50,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd3, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "READY", Some(50))));

    // Verify job 1 (now pri 50) is reserved before job 2 (pri 75)
    let reserve = Command::ReserveWithTimeout { timeout: 0 };
    let resp = s.handle_command(c, reserve, None);
    match resp {
        Response::Reserved { id, .. } => assert_eq!(id, 1),
        other => panic!("expected Reserved, got {:?}", other),
    }
}

#[test]
fn test_idempotency_priority_upgrade_no_downgrade() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put job at pri 50
    let cmd1 = Command::Put {
        pri: 50,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Duplicate put at pri 100 — should NOT downgrade
    let cmd2 = Command::Put {
        pri: 100,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "READY", None)));

    // Verify job still has pri 50
    assert_eq!(s.jobs.get(&1).unwrap().priority, 50);
}

#[test]
fn test_idempotency_priority_upgrade_reserved() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put and reserve a job at pri 100
    let cmd1 = Command::Put {
        pri: 100,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    let reserve = Command::ReserveWithTimeout { timeout: 0 };
    s.handle_command(c, reserve, None);

    // Duplicate put at pri 30 — should upgrade even though reserved
    let cmd2 = Command::Put {
        pri: 30,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(
        resp,
        Response::InsertedDup(1, "RESERVED", Some(30))
    ));

    // Verify the job's priority was updated
    assert_eq!(s.jobs.get(&1).unwrap().priority, 30);
}

#[test]
fn test_idempotency_priority_upgrade_buried() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put, reserve, and bury a job at pri 100
    let cmd1 = Command::Put {
        pri: 100,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    let reserve = Command::ReserveWithTimeout { timeout: 0 };
    s.handle_command(c, reserve, None);
    let bury = Command::Bury { id: 1, pri: 100 };
    s.handle_command(c, bury, None);

    // Duplicate put at pri 20 — should upgrade buried job
    let cmd2 = Command::Put {
        pri: 20,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "BURIED", Some(20))));
    assert_eq!(s.jobs.get(&1).unwrap().priority, 20);
}

#[test]
fn test_idempotency_priority_upgrade_delayed() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put a delayed job at pri 100
    let cmd1 = Command::Put {
        pri: 100,
        delay: 3600,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Duplicate put at pri 40 — should upgrade delayed job
    let cmd2 = Command::Put {
        pri: 40,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(
        resp,
        Response::InsertedDup(1, "DELAYED", Some(40))
    ));
    assert_eq!(s.jobs.get(&1).unwrap().priority, 40);
}

/// A priority upgrade on a delayed job journals its delay with the new
/// priority. It journalled the full delay, stamped at the upgrade, so a
/// restart started the countdown over from there.
#[test]
fn a_priority_upgrade_keeps_a_delayed_jobs_countdown_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(dir.path(), Duration::from_millis(100));
    let c = register(&mut s);
    let put = |pri| Command::Put {
        pri,
        delay: 3600,
        ttr: 10,
        bytes: 1,
        idempotency_key: Some(("k".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    assert_eq!(
        s.handle_command(c, put(100), Some(b"a".to_vec())),
        Response::Inserted(1)
    );
    // Most of the hour has gone by.
    s.jobs.get_mut(&1).unwrap().deadline_at = Some(Instant::now() + Duration::from_secs(60));
    assert_eq!(
        s.handle_command(c, put(40), Some(b"a".to_vec())),
        Response::InsertedDup(1, "DELAYED", Some(40))
    );
    s.wal.as_mut().unwrap().sync().unwrap();
    drop(s);

    let mut wal = crate::wal::Wal::open(dir.path(), Default::default()).unwrap();
    let (jobs, ..) = wal.replay().unwrap();
    let job = &jobs[&1];
    assert_eq!(job.priority, 40);
    let left = job
        .deadline_at
        .unwrap()
        .saturating_duration_since(Instant::now());
    assert!(
        left <= Duration::from_secs(60),
        "the countdown started over: {left:?} left"
    );
}

#[test]
fn test_idempotency_priority_upgrade_urgent_stats() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put job at pri 2000 (not urgent)
    let cmd1 = Command::Put {
        pri: 2000,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    assert_eq!(s.stats.urgent_ct, 0);

    // Duplicate put at pri 500 (urgent, < 1024) — should update urgent count
    let cmd2 = Command::Put {
        pri: 500,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "READY", Some(500))));
    assert_eq!(s.stats.urgent_ct, 1);
}

#[test]
fn test_stats_group_not_found() {
    let s = make_state();
    let resp = s.cmd_stats_group("nonexistent");
    assert!(matches!(resp, Response::NotFound));
}

#[test]
fn test_stats_group_pending() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: None,
        group: Some("g1".into()),
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));

    let resp = s.cmd_stats_group("g1");
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("name: \"g1\""), "yaml: {}", yaml);
            assert!(yaml.contains("ready: 1"), "yaml: {}", yaml);
            assert!(yaml.contains("reserved: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("delayed: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("buried: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("waiting-jobs: 0"), "yaml: {}", yaml);
        }
        _ => panic!("expected Ok, got {:?}", resp),
    }
}

#[test]
fn test_chained_aft_pipeline() {
    // Verify the ETL pipeline scenario: extract -> transform -> load
    // where each stage is held until the previous stage's group completes.
    let mut s = make_state();
    let c = register(&mut s);

    // Step 1: Put 2 extract jobs tagged with grp:extract
    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: None,
            group: Some("extract".into()),
            after_group: None,
            concurrency_key: None,
        },
        Some(b"ext-1".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(1)));

    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: None,
            group: Some("extract".into()),
            after_group: None,
            concurrency_key: None,
        },
        Some(b"ext-2".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(2)));

    // Step 2: Put 1 transform job with aft:extract grp:transform
    // It should be held because the extract group is not yet complete.
    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: None,
            group: Some("transform".into()),
            after_group: Some("extract".into()),
            concurrency_key: None,
        },
        Some(b"xform".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(3)));

    // Step 3: Put 1 load job with aft:transform
    // It should be held because the transform group is not yet complete.
    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: None,
            group: None,
            after_group: Some("transform".into()),
            concurrency_key: None,
        },
        Some(b"load!".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(4)));

    // Step 4: Verify the transform job (3) and load job (4) are not reservable
    // while the extract group still has pending jobs.
    // Only extract jobs 1 and 2 should be available.
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::Reserved { id: 1, .. }),
        "expected extract job 1, got {:?}",
        r
    );
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::Reserved { id: 2, .. }),
        "expected extract job 2, got {:?}",
        r
    );
    // No more ready jobs -- transform is still held
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::TimedOut),
        "expected TimedOut (transform held), got {:?}",
        r
    );

    // Step 5: Delete both extract jobs. This completes the extract group,
    // which should promote the transform job to ready.
    s.handle_command(c, Command::Delete { id: 1 }, None);
    s.handle_command(c, Command::Delete { id: 2 }, None);

    // Step 6: Verify the transform job is now reservable.
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::Reserved { id: 3, .. }),
        "expected transform job 3 after extract completes, got {:?}",
        r
    );
    // Load job is still held -- transform group not yet complete
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::TimedOut),
        "expected TimedOut (load held), got {:?}",
        r
    );

    // Step 7: Delete the transform job. This completes the transform group,
    // which should promote the load job to ready.
    s.handle_command(c, Command::Delete { id: 3 }, None);

    // Step 8: Verify the load job is now reservable.
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::Reserved { id: 4, .. }),
        "expected load job 4 after transform completes, got {:?}",
        r
    );
}

#[test]
fn test_stats_group_buried() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: None,
        group: Some("g1".into()),
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));

    // Reserve and bury
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None);

    let resp = s.cmd_stats_group("g1");
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("ready: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("reserved: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("delayed: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("buried: 1"), "yaml: {}", yaml);
        }
        _ => panic!("expected Ok, got {:?}", resp),
    }
}

#[test]
fn test_stats_job_idempotency_ttl() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 30)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));

    let resp = s.handle_command(c, Command::StatsJob { id: 1 }, None);
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("idempotency-key: mykey"), "yaml: {}", yaml);
            assert!(yaml.contains("idempotency-ttl: 30"), "yaml: {}", yaml);
        }
        _ => panic!("expected Ok"),
    }
}

#[test]
fn test_delete_batch_all_reserved() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put and reserve 3 jobs
    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"a".to_vec()));
    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"b".to_vec()));
    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"c".to_vec()));
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Reserve, None);

    let resp = s.handle_command(c, Command::DeleteBatch { ids: vec![1, 2, 3] }, None);
    assert_eq!(
        resp,
        Response::DeletedBatch {
            deleted: 3,
            not_found: 0
        }
    );
}

#[test]
fn test_delete_batch_mixed() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"a".to_vec()));
    s.handle_command(c, Command::Reserve, None);

    let resp = s.handle_command(
        c,
        Command::DeleteBatch {
            ids: vec![1, 999, 1000],
        },
        None,
    );
    assert_eq!(
        resp,
        Response::DeletedBatch {
            deleted: 1,
            not_found: 2
        }
    );
}

#[test]
fn test_delete_batch_all_not_found() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(c, Command::DeleteBatch { ids: vec![99, 100] }, None);
    assert_eq!(
        resp,
        Response::DeletedBatch {
            deleted: 0,
            not_found: 2
        }
    );
}

#[test]
fn test_delete_batch_other_conn_reserved() {
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);

    // c1 reserves a job
    s.handle_command(c1, put_cmd(0, 0, 10, 1), Some(b"a".to_vec()));
    s.handle_command(c1, Command::Reserve, None);

    // c2 tries to delete-batch it — should be not_found (not reserved by c2)
    let resp = s.handle_command(c2, Command::DeleteBatch { ids: vec![1] }, None);
    assert_eq!(
        resp,
        Response::DeletedBatch {
            deleted: 0,
            not_found: 1
        }
    );
}

// --- Memory accounting (--max-jobs-size) ---

/// Sum the live memory cost of every job and tombstone from scratch.
/// Must match `state.total_job_bytes` at all times.
fn recompute_total_job_bytes(state: &ServerState) -> u64 {
    let jobs: u64 = state.jobs.values().map(|j| ServerState::job_memory_cost(j)).sum();
    let tombs: u64 = state
        .tubes
        .values()
        .flat_map(|t| t.idempotency_cooldowns.keys())
        .map(|k| ServerState::tombstone_memory_cost(k))
        .sum();
    jobs + tombs
}

fn make_state_with_limit(max_bytes: Option<u64>) -> ServerState {
    ServerState::new(
        65535,
        max_bytes,
        None,
        None,
        Arc::new(FdBudget::new(Some(0))),
    )
}

#[test]
fn test_memory_accounting_put_delete_zero() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 60, 5), Some(b"hello".to_vec()));
    s.handle_command(c, put_cmd(0, 0, 60, 3), Some(b"foo".to_vec()));
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));
    assert_eq!(s.total_job_bytes, (5 + 512) + (3 + 512));

    s.handle_command(c, Command::Delete { id: 1 }, None);
    s.handle_command(c, Command::Delete { id: 2 }, None);
    assert_eq!(s.total_job_bytes, 0);
    assert_eq!(recompute_total_job_bytes(&s), 0);
}

#[test]
fn test_memory_accounting_reserve_release_is_neutral() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 60, 5), Some(b"hello".to_vec()));
    let baseline = s.total_job_bytes;

    let resp = s.handle_command(c, Command::Reserve, None);
    let id = match resp {
        Response::Reserved { id, .. } => id,
        _ => panic!("expected Reserved"),
    };
    // Reserve doesn't add bytes.
    assert_eq!(s.total_job_bytes, baseline);

    s.handle_command(
        c,
        Command::Release {
            id,
            pri: 0,
            delay: 0,
        },
        None,
    );
    assert_eq!(s.total_job_bytes, baseline);
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));
}

#[test]
fn test_memory_accounting_bury_kick_is_neutral() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 60, 5), Some(b"hello".to_vec()));
    let baseline = s.total_job_bytes;

    let resp = s.handle_command(c, Command::Reserve, None);
    let id = match resp {
        Response::Reserved { id, .. } => id,
        _ => panic!("expected Reserved"),
    };
    s.handle_command(c, Command::Bury { id, pri: 0 }, None);
    assert_eq!(s.total_job_bytes, baseline);

    s.handle_command(c, Command::Kick { bound: 1 }, None);
    assert_eq!(s.total_job_bytes, baseline);
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));
}

#[test]
fn test_memory_limit_blocks_put() {
    // Budget for exactly one 10-byte job (10 + 512 = 522). A second put
    // of the same size must be rejected.
    let mut s = make_state_with_limit(Some(522));
    let c = register(&mut s);

    let r1 = s.handle_command(c, put_cmd(0, 0, 60, 10), Some(vec![b'a'; 10]));
    assert!(matches!(r1, Response::Inserted(1)), "first put: {r1:?}");

    let r2 = s.handle_command(c, put_cmd(0, 0, 60, 10), Some(vec![b'b'; 10]));
    assert!(matches!(r2, Response::OutOfMemory), "second put: {r2:?}");

    // Delete frees the budget; next put should succeed again.
    s.handle_command(c, Command::Delete { id: 1 }, None);
    assert_eq!(s.total_job_bytes, 0);

    let r3 = s.handle_command(c, put_cmd(0, 0, 60, 10), Some(vec![b'c'; 10]));
    assert!(
        matches!(r3, Response::Inserted(2)),
        "post-delete put: {r3:?}"
    );
}

#[test]
fn test_memory_limit_allows_reserve_at_capacity() {
    let mut s = make_state_with_limit(Some(522));
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 60, 10), Some(vec![b'a'; 10]));
    // At the limit. Reserve must still succeed.
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(matches!(resp, Response::Reserved { .. }));

    // And release, too.
    let resp = s.handle_command(
        c,
        Command::Release {
            id: 1,
            pri: 0,
            delay: 0,
        },
        None,
    );
    assert!(matches!(resp, Response::Released));
}

#[test]
fn test_memory_limit_put_with_idp_accounts_for_tombstone() {
    // A 5-byte body + 512 job overhead + "k1" tombstone (2 + 128 = 130)
    // = 647. Allow exactly that, put once. Then put a second fresh idp:
    // 5 + 512 = 517 of job + "k2" 2 + 128 = 130 of tombstone = 647 more.
    // Budget of 647 fits exactly one such put, second must fail.
    let mut s = make_state_with_limit(Some(647));
    let c = register(&mut s);

    let cmd_idp1 = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("k1".to_string(), 30)), // TTL > 0 → tombstone cost
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let r1 = s.handle_command(c, cmd_idp1, Some(b"hello".to_vec()));
    assert!(matches!(r1, Response::Inserted(1)), "got {r1:?}");

    let cmd_idp2 = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("k2".to_string(), 30)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let r2 = s.handle_command(c, cmd_idp2, Some(b"world".to_vec()));
    assert!(matches!(r2, Response::OutOfMemory), "got {r2:?}");
}

#[test]
fn test_memory_accounting_flush_tube_drains_tombstones() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put with idp+TTL, then delete → creates cooldown tombstone.
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("k1".to_string(), 30)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));
    s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(
        s.total_job_bytes > 0,
        "tombstone should remain after delete"
    );
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));

    s.handle_command(
        c,
        Command::FlushTube {
            tube: "default".to_string(),
        },
        None,
    );
    assert_eq!(s.total_job_bytes, 0);
    assert_eq!(recompute_total_job_bytes(&s), 0);
}

#[test]
fn test_memory_accounting_churn_returns_to_zero() {
    // Mixed stream of puts, deletes, reserves, releases. After everything
    // is deleted, the counter must match a from-scratch recomputation
    // (0 in this case since all jobs are gone).
    let mut s = make_state();
    let c = register(&mut s);

    let mut live_ids: Vec<u64> = Vec::new();

    for i in 0..500u32 {
        let body = format!("job-{i}").into_bytes();
        let cmd = put_cmd(0, 0, 60, body.len() as u32);
        if let Response::Inserted(id) = s.handle_command(c, cmd, Some(body)) {
            live_ids.push(id);
        }
    }
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));

    // Reserve+release a batch — net zero.
    for _ in 0..100 {
        if let Response::Reserved { id, .. } = s.handle_command(c, Command::Reserve, None) {
            s.handle_command(
                c,
                Command::Release {
                    id,
                    pri: 0,
                    delay: 0,
                },
                None,
            );
        }
    }
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));

    // Reserve+bury+kick a batch — net zero.
    for _ in 0..50 {
        if let Response::Reserved { id, .. } = s.handle_command(c, Command::Reserve, None) {
            s.handle_command(c, Command::Bury { id, pri: 0 }, None);
        }
    }
    s.handle_command(c, Command::Kick { bound: 100 }, None);
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));

    // Delete every remaining job.
    for id in live_ids {
        s.handle_command(c, Command::Delete { id }, None);
    }
    assert_eq!(s.total_job_bytes, 0);
    assert_eq!(recompute_total_job_bytes(&s), 0);
}

#[test]
fn test_drift_detector_fires_and_self_heals_on_empty_state() {
    let mut s = make_state();
    // Simulate a missed accounting decrement: counter non-zero, no jobs.
    s.total_job_bytes = 9999;
    assert_eq!(s.stats.accounting_drift_events, 0);

    s.tick();

    assert_eq!(
        s.total_job_bytes, 0,
        "drift detector should reset counter to 0"
    );
    assert_eq!(
        s.stats.accounting_drift_events, 1,
        "drift detector should bump the counter"
    );
}

#[test]
fn test_drift_detector_does_not_fire_with_live_jobs() {
    let mut s = make_state();
    let c = register(&mut s);
    s.handle_command(c, put_cmd(0, 0, 60, 5), Some(b"hello".to_vec()));
    let baseline = s.total_job_bytes;

    // Corrupt the counter upward. With a live job present, the drift
    // detector must not touch it — we can't tell if the extra bytes are
    // drift or real.
    s.total_job_bytes += 1000;
    let corrupted = s.total_job_bytes;

    s.tick();

    assert_eq!(
        s.total_job_bytes, corrupted,
        "detector must only fire on empty state"
    );
    assert_eq!(s.stats.accounting_drift_events, 0);
    // Clean up so the test doesn't leak a weird counter.
    s.total_job_bytes = baseline;
}

#[test]
fn test_parse_bytes_variants() {
    assert_eq!(parse_bytes("1024"), Ok(1024));
    assert_eq!(parse_bytes("1k"), Ok(1024));
    assert_eq!(parse_bytes("1K"), Ok(1024));
    assert_eq!(parse_bytes("2m"), Ok(2 * 1024 * 1024));
    assert_eq!(parse_bytes("2M"), Ok(2 * 1024 * 1024));
    assert_eq!(parse_bytes("3g"), Ok(3 * 1024 * 1024 * 1024));
    assert_eq!(parse_bytes("3G"), Ok(3 * 1024 * 1024 * 1024));
    assert_eq!(parse_bytes("1t"), Ok(1024u64.pow(4)));
    assert_eq!(parse_bytes("2gb"), Ok(2 * 1024 * 1024 * 1024));
    assert_eq!(parse_bytes("2GB"), Ok(2 * 1024 * 1024 * 1024));
    assert_eq!(parse_bytes("500M"), Ok(500 * 1024 * 1024));
    assert!(parse_bytes("").is_err());
    assert!(parse_bytes("nope").is_err());
    assert!(parse_bytes("1.5g").is_err()); // decimals not supported
}

#[test]
fn a_released_aft_job_is_not_held_again_after_a_delayed_release() {
    // Job 1 is put before group g has a job, so it is released at once.
    // Job 2 then joins g. That doesn't hold job 1 again when a release with
    // a delay puts it back in the delay heap, however it comes out.
    let mut s = make_state();
    let c = register(&mut s);
    let put = |s: &mut ServerState, pri: u32, group: Option<&str>, after: Option<&str>| {
        let cmd = Command::Put {
            pri,
            delay: 0,
            ttr: 120,
            bytes: 1,
            idempotency_key: None,
            group: group.map(Into::into),
            after_group: after.map(Into::into),
            concurrency_key: None,
        };
        s.handle_command(c, cmd, Some(b"x".to_vec()))
    };
    assert_eq!(put(&mut s, 0, None, Some("g")), Response::Inserted(1));
    assert_eq!(put(&mut s, 10, Some("g"), None), Response::Inserted(2));
    let reserve_and_delay = |s: &mut ServerState| {
        assert!(matches!(
            s.handle_command(c, Command::Reserve, None),
            Response::Reserved { id: 1, .. }
        ));
        assert_eq!(
            s.handle_command(
                c,
                Command::Release {
                    id: 1,
                    pri: 0,
                    delay: 60
                },
                None
            ),
            Response::Released
        );
    };

    reserve_and_delay(&mut s);
    make_due(&mut s, 1);
    s.tick();
    assert_eq!(s.jobs[&1].state, JobState::Ready, "due");

    reserve_and_delay(&mut s);
    assert_eq!(
        s.handle_command(c, Command::Kick { bound: 1 }, None),
        Response::Kicked(1)
    );
    assert_eq!(s.jobs[&1].state, JobState::Ready, "kick");

    reserve_and_delay(&mut s);
    assert_eq!(
        s.handle_command(c, Command::KickJob { id: 1 }, None),
        Response::KickedOne
    );

    reserve_and_delay(&mut s);
    assert!(matches!(
        s.handle_command(c, Command::ReserveJob { id: 1 }, None),
        Response::Reserved { id: 1, .. }
    ));
}

#[test]
fn test_reserve_job_respects_after_group() {
    // reserve-job by ID must not bypass after_group dependencies.
    let mut s = make_state();
    let c = register(&mut s);

    // Put a predecessor job in group "g1"
    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 4,
            idempotency_key: None,
            group: Some("g1".into()),
            after_group: None,
            concurrency_key: None,
        },
        Some(b"pre1".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(1)));

    // Put a dependent job with aft:g1
    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 4,
            idempotency_key: None,
            group: None,
            after_group: Some("g1".into()),
            concurrency_key: None,
        },
        Some(b"dep1".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(2)));

    // reserve-job 2 should fail — g1 is not complete
    let r = s.handle_command(c, Command::ReserveJob { id: 2 }, None);
    assert!(
        matches!(r, Response::NotFound),
        "reserve-job should block on incomplete after_group, got {:?}",
        r
    );

    // Complete g1: reserve and delete job 1
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(matches!(r, Response::Reserved { id: 1, .. }));
    let r = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(r, Response::Deleted));

    // Now reserve-job 2 should succeed
    let r = s.handle_command(c, Command::ReserveJob { id: 2 }, None);
    assert!(
        matches!(r, Response::Reserved { id: 2, .. }),
        "reserve-job should succeed after group completes, got {:?}",
        r
    );
}

// --- waiting_ct accounting ---

#[test]
fn test_waiting_ct_reserve_blocks() {
    let mut s = make_state();
    let c = register(&mut s);
    let (tx, _rx) = oneshot::channel();
    s.add_waiter(c, tx, None, None);
    assert_eq!(s.stats.waiting_ct, 1);
    assert_eq!(s.tubes.get("default").unwrap().stat.waiting_ct, 1);
}

#[test]
fn test_waiting_ct_fulfilled_waiter_resets() {
    let mut s = make_state();
    let c = register(&mut s);
    let (tx, _rx) = oneshot::channel();
    s.add_waiter(c, tx, None, None);
    assert_eq!(s.stats.waiting_ct, 1);

    // Put a job — process_queue should wake the waiter
    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    assert_eq!(s.stats.waiting_ct, 0);
    assert_eq!(s.tubes.get("default").unwrap().stat.waiting_ct, 0);
}

#[test]
fn test_waiting_ct_disconnect_clears() {
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);

    // c2 watches an extra tube
    s.handle_command(
        c2,
        Command::Watch {
            tube: "foo".into(),
            weight: 1,
        },
        None,
    );

    let (tx1, _rx1) = oneshot::channel();
    s.add_waiter(c1, tx1, None, None);
    let (tx2, _rx2) = oneshot::channel();
    s.add_waiter(c2, tx2, None, None);

    assert_eq!(s.stats.waiting_ct, 2);
    assert_eq!(s.tubes.get("default").unwrap().stat.waiting_ct, 2);
    assert_eq!(s.tubes.get("foo").unwrap().stat.waiting_ct, 1);

    // Disconnect c1 — its waiter should be removed
    s.unregister_conn(c1);
    assert_eq!(s.stats.waiting_ct, 1);
    assert_eq!(s.tubes.get("default").unwrap().stat.waiting_ct, 1);
    assert_eq!(s.tubes.get("foo").unwrap().stat.waiting_ct, 1);
}

// Point an already-ready job at a non-existent external body. make_state has no
// body_store, so fetch_body returns None and do_reserve yields INTERNAL_ERROR —
// simulating a TOAST read failure without a real on-disk store.
fn poison_body(s: &mut ServerState, id: u64) {
    s.jobs.get_mut(&id).unwrap().body = BodyRef::External(BodyId(9999), None);
}

#[test]
fn test_reserve_batch_surfaces_body_store_error() {
    // The only ready job has an unreadable body. A batch reserve must surface
    // INTERNAL_ERROR, not mask the storage fault as an empty RESERVED_BATCH.
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(c, put_cmd(0, 0, 10, 4), Some(b"poof".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));
    poison_body(&mut s, 1);

    let resp = s.handle_command(
        c,
        Command::ReserveBatch {
            count: 5,
            timeout: None,
        },
        None,
    );
    assert!(
        matches!(resp, Response::InternalError),
        "expected INTERNAL_ERROR, got {resp:?}"
    );
}

#[test]
fn test_reserve_batch_partial_wins_over_body_store_error() {
    // A healthy job precedes the poison one (same pri ⇒ ordered by id). The
    // batch returns the good job; the unreadable job is auto-buried so it can't
    // wedge the ready heap, and the error is suppressed for this partial-success
    // call.
    let mut s = make_state();
    let c = register(&mut s);

    assert!(matches!(
        s.handle_command(c, put_cmd(0, 0, 10, 4), Some(b"good".to_vec())),
        Response::Inserted(1)
    ));
    assert!(matches!(
        s.handle_command(c, put_cmd(0, 0, 10, 4), Some(b"poof".to_vec())),
        Response::Inserted(2)
    ));
    poison_body(&mut s, 2);

    let resp = s.handle_command(
        c,
        Command::ReserveBatch {
            count: 5,
            timeout: None,
        },
        None,
    );
    match resp {
        Response::ReservedBatch(jobs) => {
            assert_eq!(jobs.len(), 1, "only the healthy job should be returned");
            assert_eq!(jobs[0].0, 1);
        }
        other => panic!("expected RESERVED_BATCH with one job, got {other:?}"),
    }
    // The unreadable job is auto-buried (not left ready to re-wedge the tube).
    assert_eq!(s.jobs.get(&2).unwrap().state, JobState::Buried);
}

#[test]
fn an_unreadable_job_buried_by_a_reserve_stops_holding_its_con_key() {
    let mut s = make_state();
    let c = register(&mut s);
    s.handle_command(c, put_con_cmd(0, 10, 1, "k", 1), Some(b"a".to_vec()));
    s.handle_command(c, put_con_cmd(0, 10, 1, "k", 3), Some(b"b".to_vec()));
    poison_body(&mut s, 1);

    // Nothing was reserved before the failed read, so the batch reports it.
    let resp = s.handle_command(
        c,
        Command::ReserveBatch {
            count: 5,
            timeout: None,
        },
        None,
    );
    assert_eq!(resp, Response::InternalError);
    assert_eq!(s.jobs.get(&1).unwrap().state, JobState::Buried);
    let declared: Vec<u32> = s.concurrency_limits["k"].keys().copied().collect();
    assert_eq!(declared, [3], "a buried job's limit doesn't count");
}

fn make_wal_state(dir: &std::path::Path, sync_interval: Duration) -> ServerState {
    let mut s = make_state();
    let config = crate::wal::WalConfig {
        sync_interval,
        ..Default::default()
    };
    s.wal = Some(crate::wal::Wal::open(dir, config).unwrap());
    s
}

fn send_put(
    s: &mut ServerState,
    strict: bool,
    conn_id: u64,
    body: &[u8],
    pending: &mut Vec<(oneshot::Sender<Response>, Response)>,
) -> oneshot::Receiver<Response> {
    let (reply_tx, reply_rx) = oneshot::channel();
    let msg = EngineMsg {
        conn_id,
        payload: EnginePayload::Command {
            cmd: put_cmd(0, 0, 60, body.len() as u32),
            body: Some(body.to_vec()),
            reply_tx,
        },
    };
    process_message(s, strict, msg, pending);
    reply_rx
}

/// Strict mode, one group-commit batch: put 1 is buffered and waiting on the
/// batch fsync when put 2's WAL write fails. Neither is durable, so neither
/// may be acked INSERTED. This used to ack both: the failure set `wal = None`,
/// put 2 then went out as an immediate INSERTED, and `sync_wal` reported a
/// missing WAL as a successful sync, so put 1's deferred ack went out too.
#[test]
fn test_strict_batch_wal_write_failure_acks_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(dir.path(), Duration::ZERO);
    let c = register(&mut s);
    let strict = s.wal_is_strict();
    assert!(strict);

    let mut pending = Vec::new();
    let _rx1 = send_put(&mut s, strict, c, b"a", &mut pending);
    assert_eq!(pending.len(), 1, "put 1 waits on the batch fsync");

    s.wal.as_mut().unwrap().inject_io_failure();
    let mut rx2 = send_put(&mut s, strict, c, b"b", &mut pending);

    assert!(
        !s.sync_wal(),
        "the batch's writes are not durable, so its acks must fail"
    );
    let r2 = rx2.try_recv();
    assert!(
        matches!(r2, Ok(Response::InternalError)),
        "the put whose write failed must not be acked, got {r2:?}"
    );
}

/// Once the WAL has failed, puts are refused: there is nowhere to persist
/// them. Holds in relaxed mode too, where the failure is usually first seen
/// by the tick's backstop fsync. State changes on existing jobs still apply
/// in memory: at worst a crash redelivers those jobs.
#[test]
fn test_wal_failure_refuses_later_puts() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(dir.path(), Duration::from_millis(100));
    let c = register(&mut s);

    let resp = s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"a".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)), "got {resp:?}");

    s.wal.as_mut().unwrap().inject_io_failure();
    assert!(!s.sync_wal(), "the backstop fsync fails");

    let resp = s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"b".to_vec()));
    assert!(
        matches!(resp, Response::InternalError),
        "a put after the WAL failed must be refused, got {resp:?}"
    );

    let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted), "got {resp:?}");
}

/// Relaxed mode: a put whose WAL write fails is answered INTERNAL_ERROR, so
/// it must not run either. It used to be queued anyway and handed straight
/// to a parked worker, so a producer retrying after the error got it run
/// twice.
#[test]
fn a_put_whose_wal_write_fails_is_not_run() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(dir.path(), Duration::from_millis(100));
    let (producer, worker) = (register(&mut s), register(&mut s));
    let (tx, mut rx) = oneshot::channel();
    s.add_waiter(worker, tx, None, None);

    s.wal.as_mut().unwrap().inject_io_failure();
    let resp = s.handle_command(producer, put_cmd(0, 0, 60, 1), Some(b"a".to_vec()));
    assert_eq!(resp, Response::InternalError);
    assert!(rx.try_recv().is_err(), "the worker must not get the job");
    assert!(s.jobs.is_empty(), "nor may it wait in a queue");
    assert_eq!(ready_in(&s, "default"), 0);
}

/// Many tubes, and one worker parked on `jobs` alone.
fn many_tubes_and_a_worker_parked_on_one() -> (ServerState, u64, oneshot::Receiver<Response>) {
    let mut s = make_state();
    for i in 0..10_000 {
        s.ensure_tube(&format!("t{i}"));
    }
    let (producer, worker) = (register(&mut s), register(&mut s));
    s.handle_command(
        worker,
        Command::Watch {
            tube: "jobs".into(),
            weight: 1,
        },
        None,
    );
    s.handle_command(
        worker,
        Command::Ignore {
            tube: "default".into(),
        },
        None,
    );
    let (tx, rx) = oneshot::channel();
    s.add_waiter(worker, tx, None, None);
    (s, producer, rx)
}

/// Tubes searched for a ready job while `f` runs.
fn tubes_looked_at(f: impl FnOnce()) -> u64 {
    let before = TUBES_LOOKED_AT.with(|n| n.get());
    f();
    TUBES_LOOKED_AT.with(|n| n.get()) - before
}

#[test]
fn a_put_looks_for_a_parked_worker_only_in_its_own_tube() {
    // With any worker parked, each put searched every tube on the server,
    // whether or not the worker watched the put's.
    let (mut s, producer, mut rx) = many_tubes_and_a_worker_parked_on_one();
    let n = tubes_looked_at(|| {
        s.handle_command(producer, put_cmd(0, 0, 60, 1), Some(b"a".to_vec()));
    });
    assert!(
        n < 10,
        "a put into a tube nobody waits on searched {n} tubes"
    );
    assert!(rx.try_recv().is_err());

    s.handle_command(
        producer,
        Command::Use {
            tube: "jobs".into(),
        },
        None,
    );
    let n = tubes_looked_at(|| {
        s.handle_command(producer, put_cmd(0, 0, 60, 1), Some(b"b".to_vec()));
    });
    assert!(n < 10, "a put for the parked worker searched {n} tubes");
    assert!(matches!(
        rx.try_recv(),
        Ok(Response::Reserved { id: 2, .. })
    ));
}

#[test]
fn a_pass_over_parked_workers_looks_only_in_tubes_they_watch() {
    // The tick's pass, and the one after a delete, release, kick or unpause.
    let (mut s, _, _rx) = many_tubes_and_a_worker_parked_on_one();
    let n = tubes_looked_at(|| s.process_queue());
    assert!(n < 10, "searched {n} tubes for one parked worker");
}

#[test]
fn a_freed_job_goes_to_the_longest_waiting_worker_that_watches_its_tube() {
    let mut s = make_state();
    let producer = register(&mut s);
    let mut parked = Vec::new();
    for watches in [&["x"][..], &["y", "x"], &["y"]] {
        let c = register(&mut s);
        s.handle_command(
            c,
            Command::Ignore {
                tube: "default".into(),
            },
            None,
        );
        for &t in watches {
            s.handle_command(
                c,
                Command::Watch {
                    tube: t.into(),
                    weight: 1,
                },
                None,
            );
        }
        let (tx, rx) = oneshot::channel();
        s.add_waiter(c, tx, None, None);
        parked.push(rx);
    }
    for t in ["x", "y"] {
        s.handle_command(
            producer,
            Command::PauseTube {
                tube: t.into(),
                delay: 60,
            },
            None,
        );
        s.handle_command(producer, Command::Use { tube: t.into() }, None);
        s.handle_command(producer, put_cmd(0, 0, 60, 1), Some(t.as_bytes().to_vec()));
    }
    for t in ["x", "y"] {
        s.tubes.get_mut(t).unwrap().unpause_at = None;
    }

    s.process_queue();
    // The first worker takes x's job; y's goes to the second, which arrived
    // before the third.
    let got: Vec<Option<u64>> = parked
        .iter_mut()
        .map(|rx| match rx.try_recv() {
            Ok(Response::Reserved { id, .. }) => Some(id),
            _ => None,
        })
        .collect();
    assert_eq!(got, [Some(1), Some(2), None]);
}

#[test]
#[ignore = "timing, run by hand"]
fn puts_stay_cheap_with_a_worker_parked_on_many_tubes() {
    // The review's case: a worker parked while watching 200,000 tubes, and
    // each put searching every one of them for it. Run with
    // `cargo test --release --lib puts_stay_cheap -- --ignored --nocapture`.
    let time_puts = |park: bool| {
        let mut s = make_state();
        let (producer, worker) = (register(&mut s), register(&mut s));
        for i in 0..200_000 {
            let name = format!("t{i}");
            s.ensure_tube(&name);
            let conn = s.conns.get_mut(&worker).unwrap();
            conn.watch(&name, 1);
        }
        let _rx = park.then(|| {
            let (tx, rx) = oneshot::channel();
            s.add_waiter(worker, tx, None, None);
            rx
        });
        s.handle_command(
            producer,
            Command::Use {
                tube: "elsewhere".into(),
            },
            None,
        );
        let start = Instant::now();
        for _ in 0..1_000 {
            s.handle_command(producer, put_cmd(0, 0, 60, 1), Some(b"x".to_vec()));
        }
        start.elapsed() / 1_000
    };
    let idle = time_puts(false);
    let parked = time_puts(true);
    eprintln!("per put: {idle:?} with no worker parked, {parked:?} with one parked on 200k tubes");
    assert!(parked < idle * 10, "{parked:?} against {idle:?}");
}

#[test]
#[ignore = "timing, run by hand"]
fn watching_and_ignoring_many_tubes_stays_linear() {
    // `watch` and `ignore` looked for the tube in the connection's whole
    // watch list, so watching N tubes one at a time took O(N²) time: 41 s
    // for 200,000. Run with
    // `cargo test --release --lib watching_and_ignoring -- --ignored --nocapture`.
    let time = |n: usize| {
        let mut s = make_state();
        let conn = register(&mut s);
        let names: Vec<String> = (0..n).map(|i| format!("t{i}")).collect();
        for name in &names {
            s.ensure_tube(name);
        }
        let start = Instant::now();
        for name in &names {
            let tube = name.clone();
            s.handle_command(conn, Command::Watch { tube, weight: 1 }, None);
        }
        let watch = start.elapsed();
        let start = Instant::now();
        for name in &names {
            let tube = name.clone();
            s.handle_command(conn, Command::Ignore { tube }, None);
        }
        (watch, start.elapsed())
    };
    let (watch1, ignore1) = time(25_000);
    let (watch2, ignore2) = time(50_000);
    eprintln!(
        "watch: {watch1:?} for 25k tubes, {watch2:?} for 50k; ignore: {ignore1:?}, {ignore2:?}"
    );
    assert!(watch2 < watch1 * 3, "watch: {watch1:?} then {watch2:?}");
    assert!(
        ignore2 < ignore1 * 3,
        "ignore: {ignore1:?} then {ignore2:?}"
    );
}

#[test]
fn no_parked_worker_could_take_a_job_after_any_command_under_random_ops() {
    // A put looks only at its own tube for a parked worker. That is enough
    // only if, after every command, no parked worker could take a job
    // anywhere. Random puts, reserves, parks, deletes, releases, buries,
    // kicks, delays coming due, pauses and disconnects across three tubes
    // sharing con: keys; after each step, check that, and each tube's index
    // of the reserves parked on it.
    const TUBES: [&str; 3] = ["a", "b", "c"];
    let mut served = 0;
    for seed in 1..=8u64 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut next = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        let mut s = make_state();
        let producer = register(&mut s);
        let new_worker = |s: &mut ServerState, next: &mut dyn FnMut(u64) -> u64| {
            let c = register(s);
            let first = next(3) as usize;
            for (i, t) in TUBES.iter().enumerate() {
                if i == first || next(3) == 0 {
                    s.handle_command(
                        c,
                        Command::Watch {
                            tube: (*t).into(),
                            weight: 1,
                        },
                        None,
                    );
                }
            }
            s.handle_command(
                c,
                Command::Ignore {
                    tube: "default".into(),
                },
                None,
            );
            c
        };
        let mut workers: Vec<u64> = (0..5).map(|_| new_worker(&mut s, &mut next)).collect();
        let mut parked: HashMap<u64, oneshot::Receiver<Response>> = HashMap::new();

        for step in 0..2000 {
            // A parked worker whose reserve was answered is free again.
            parked.retain(|_, rx| match rx.try_recv() {
                Err(oneshot::error::TryRecvError::Empty) => true,
                Ok(_) => {
                    served += 1;
                    false
                }
                Err(_) => false,
            });
            let w = workers[next(workers.len() as u64) as usize];
            let free = !parked.contains_key(&w);
            let held: Vec<u64> = s.conns[&w].reserved_jobs.clone();
            let pick = |next: &mut dyn FnMut(u64) -> u64| {
                (!held.is_empty()).then(|| held[next(held.len() as u64) as usize])
            };
            let tube = TUBES[next(3) as usize];
            match next(100) {
                0..=29 => {
                    s.handle_command(producer, Command::Use { tube: tube.into() }, None);
                    let cmd = Command::Put {
                        pri: next(4) as u32,
                        delay: if next(10) == 0 { 60 } else { 0 },
                        ttr: 120,
                        bytes: 1,
                        idempotency_key: None,
                        group: None,
                        after_group: None,
                        concurrency_key: (next(10) < 6)
                            .then(|| (["k", "l"][next(2) as usize].into(), 1 + next(2) as u32)),
                    };
                    s.handle_command(producer, cmd, Some(b"x".to_vec()));
                }
                30..=49 if free => {
                    if reserve_now(&mut s, w).is_none() {
                        let (tx, rx) = oneshot::channel();
                        s.add_waiter(w, tx, None, None);
                        parked.insert(w, rx);
                    }
                }
                50..=61 if free => {
                    if let Some(id) = pick(&mut next) {
                        s.handle_command(w, Command::Delete { id }, None);
                    }
                }
                62..=69 if free => {
                    if let Some(id) = pick(&mut next) {
                        let delay = if next(4) == 0 { 60 } else { 0 };
                        s.handle_command(w, Command::Release { id, pri: 0, delay }, None);
                    }
                }
                70..=74 if free => {
                    if let Some(id) = pick(&mut next) {
                        s.handle_command(w, Command::Bury { id, pri: 0 }, None);
                    }
                }
                75..=79 => {
                    s.handle_command(producer, Command::Use { tube: tube.into() }, None);
                    let bound = 1 + next(3) as u32;
                    s.handle_command(producer, Command::Kick { bound }, None);
                }
                80..=84 => {
                    let mut delayed: Vec<u64> = s
                        .jobs
                        .values()
                        .filter(|j| j.state == JobState::Delayed)
                        .map(|j| j.id)
                        .collect();
                    delayed.sort_unstable();
                    if !delayed.is_empty() {
                        make_due(&mut s, delayed[next(delayed.len() as u64) as usize]);
                    }
                    s.tick();
                }
                85..=89 => {
                    let delay = if next(2) == 0 { 60 } else { 0 };
                    let cmd = Command::PauseTube {
                        tube: tube.into(),
                        delay,
                    };
                    s.handle_command(producer, cmd, None);
                }
                90..=94 => {
                    s.unregister_conn(w);
                    parked.remove(&w);
                    workers.retain(|&x| x != w);
                    workers.push(new_worker(&mut s, &mut next));
                }
                _ => s.tick(),
            }

            for waiter in &s.waiters {
                assert_eq!(
                    s.find_ready_job_for_conn_inner(waiter.conn_id),
                    None,
                    "seed {seed} step {step}: conn {} is parked with a job it could take",
                    waiter.conn_id
                );
            }
            for (name, tube) in &s.tubes {
                let want: std::collections::BTreeSet<u64> = s
                    .waiters
                    .iter()
                    .filter(|w| s.conns[&w.conn_id].watches_tube(name))
                    .map(|w| w.seq)
                    .collect();
                assert_eq!(tube.waiters, want, "seed {seed} step {step} tube {name}");
                assert_eq!(tube.stat.waiting_ct, want.len() as u64);
                assert_eq!(s.waited_tubes.contains(name), !want.is_empty());
            }
            assert!(s.waited_tubes.iter().all(|t| s.tubes.contains_key(t)));
        }
    }
    eprintln!("parked reserves served: {served}");
    assert!(served > 200, "only {served} parked reserves were served");
}

/// Taking back a put its group held leaves the tube's held count as it was.
#[test]
fn a_held_put_whose_wal_write_fails_is_not_counted_as_held() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(dir.path(), Duration::from_millis(100));
    let c = register(&mut s);
    let put = |group: Option<&str>, after_group: Option<&str>| Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 1,
        idempotency_key: None,
        group: group.map(Into::into),
        after_group: after_group.map(Into::into),
        concurrency_key: None,
    };
    let resp = s.handle_command(c, put(Some("g"), None), Some(b"a".to_vec()));
    assert_eq!(resp, Response::Inserted(1));

    s.wal.as_mut().unwrap().inject_io_failure();
    let resp = s.handle_command(c, put(None, Some("g")), Some(b"b".to_vec()));
    assert_eq!(resp, Response::InternalError);
    assert_eq!(s.tubes["default"].stat.held_ct, 0);
}

/// Strict mode: a batch whose fsync fails answers its puts INTERNAL_ERROR,
/// and the worker woken by one of them INTERNAL_ERROR too. The job used to
/// stay reserved by that worker, which never heard of it, and go to the next
/// worker when its TTR ran out.
#[test]
fn a_put_in_a_strict_batch_whose_fsync_fails_is_not_run() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(dir.path(), Duration::ZERO);
    let strict = s.wal_is_strict();
    let (producer, worker) = (register(&mut s), register(&mut s));
    let (tx, mut worker_rx) = oneshot::channel();
    s.add_waiter(worker, tx, None, None);

    let mut pending = Vec::new();
    let mut put_rx = send_put(&mut s, strict, producer, b"a", &mut pending);
    s.wal.as_mut().unwrap().inject_io_failure();
    commit_batch(&mut s, &mut pending);

    assert_eq!(put_rx.try_recv(), Ok(Response::InternalError));
    assert_eq!(worker_rx.try_recv(), Ok(Response::InternalError));
    assert!(s.jobs.is_empty(), "the job must be gone, not left reserved");
    assert!(s.conns[&worker].reserved_jobs.is_empty());
    assert_eq!(s.stats.reserved_ct, 0);
}

/// A put taken back leaves nothing behind: its idp: key is free, its group
/// no longer waits on it, its con: limit is gone, and the memory accounting
/// matches the jobs left.
#[test]
fn a_put_taken_back_leaves_no_trace() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(dir.path(), Duration::ZERO);
    let strict = s.wal_is_strict();
    let c = register(&mut s);
    let (reply_tx, mut rx) = oneshot::channel();
    let msg = EngineMsg {
        conn_id: c,
        payload: EnginePayload::Command {
            cmd: Command::Put {
                pri: 0,
                delay: 0,
                ttr: 60,
                bytes: 1,
                idempotency_key: Some(("k".into(), 60)),
                group: Some("g".into()),
                after_group: None,
                concurrency_key: Some(("c".into(), 2)),
            },
            body: Some(b"a".to_vec()),
            reply_tx,
        },
    };
    let mut pending = Vec::new();
    process_message(&mut s, strict, msg, &mut pending);
    s.wal.as_mut().unwrap().inject_io_failure();
    commit_batch(&mut s, &mut pending);

    assert_eq!(rx.try_recv(), Ok(Response::InternalError));
    let tube = &s.tubes["default"];
    assert!(tube.idempotency_keys.is_empty());
    assert!(
        tube.idempotency_cooldowns.is_empty(),
        "no cooldown, as a delete would"
    );
    assert_eq!(tube.stat.total_jobs_ct, 0);
    assert_eq!(s.stats.total_jobs_ct, 0);
    assert_eq!(s.ready_ct, 0);
    assert_eq!(s.stats.urgent_ct, 0);
    assert_eq!(s.groups.get("g").map_or(0, |g| g.pending), 0);
    assert!(s.concurrency_limits.is_empty());
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));
    assert_eq!(s.total_job_bytes, 0);
}

/// The segment file a test WAL in `dir` writes to.
fn wal_segment(dir: &std::path::Path) -> std::path::PathBuf {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("binlog.")
        })
        .unwrap()
}

/// Strict mode: dropping a failed WAL flushed its write buffer, putting on
/// disk the records of a batch just answered INTERNAL_ERROR, so a restart
/// could bring them back.
#[test]
fn a_failed_strict_wal_does_not_flush_its_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(dir.path(), Duration::ZERO);
    let c = register(&mut s);
    s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"a".to_vec()));
    assert!(s.sync_wal());
    let synced = std::fs::metadata(wal_segment(dir.path())).unwrap().len();

    s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"b".to_vec()));
    s.wal.as_mut().unwrap().inject_io_failure();
    assert!(!s.sync_wal());
    assert_eq!(
        std::fs::metadata(wal_segment(dir.path())).unwrap().len(),
        synced
    );
}

/// Relaxed mode: the buffer holds writes already acked, such as a delete,
/// which a crash would lose but a failed WAL needn't. It is flushed as
/// before; dropping it would bring a deleted job back on restart.
#[test]
fn a_failed_relaxed_wal_still_flushes_acked_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(dir.path(), Duration::from_millis(100));
    let c = register(&mut s);
    s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"a".to_vec()));
    assert!(s.sync_wal());
    let synced = std::fs::metadata(wal_segment(dir.path())).unwrap().len();

    let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert_eq!(resp, Response::Deleted);
    s.wal.as_mut().unwrap().inject_io_failure();
    assert!(!s.sync_wal());
    assert!(std::fs::metadata(wal_segment(dir.path())).unwrap().len() > synced);
}

/// `stats` output, as text.
fn stats_text(s: &mut ServerState, c: u64) -> String {
    match s.handle_command(c, Command::Stats, None) {
        Response::Ok(data) => String::from_utf8(data).unwrap(),
        other => panic!("expected Ok, got {other:?}"),
    }
}

#[test]
fn test_stats_tell_a_failed_wal_from_no_wal() {
    // binlog-enabled is false both without -b and after a WAL failure, so
    // on its own a client can't tell an in-memory server from one refusing
    // puts. binlog-failed is true only for the second.
    let mut s = make_state();
    let c = register(&mut s);
    let stats = stats_text(&mut s, c);
    assert!(stats.contains("binlog-enabled: false\n"), "{stats}");
    assert!(stats.contains("binlog-failed: false\n"), "{stats}");

    let dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(dir.path(), Duration::from_millis(100));
    let c = register(&mut s);
    s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"a".to_vec()));
    let stats = stats_text(&mut s, c);
    assert!(stats.contains("binlog-enabled: true\n"), "{stats}");
    assert!(stats.contains("binlog-failed: false\n"), "{stats}");

    s.wal.as_mut().unwrap().inject_io_failure();
    assert!(!s.sync_wal());
    let stats = stats_text(&mut s, c);
    assert!(stats.contains("binlog-enabled: false\n"), "{stats}");
    assert!(stats.contains("binlog-failed: true\n"), "{stats}");
}

// --- Recurring schedules: firing ---

/// Sync `entries` — (name, tags and spec) — for owner "app" into tube "jobs".
fn sync_schedules(s: &mut ServerState, c: u64, entries: &[(&str, &str)]) {
    let resp = s.handle_command(c, Command::ScheduleBegin { owner: "app".into() }, None);
    assert_eq!(resp, Response::Syncing("app".into()));
    for (name, rest) in entries {
        let line = format!("schedule-put {name} jobs 0 0 60 2 {rest}");
        let cmd = protocol::parse_line(line.as_bytes()).unwrap();
        assert_eq!(s.handle_command(c, cmd, Some(b"{}".to_vec())), Response::Staged);
    }
    let resp = s.handle_command(c, Command::ScheduleCommit, None);
    assert!(matches!(resp, Response::Synced(_)), "got {resp:?}");
}

fn next_fire(s: &ServerState, name: &str) -> jiff::Timestamp {
    s.schedules.get("app", name).unwrap().next_fire.unwrap()
}

fn fired(s: &ServerState, name: &str) -> u64 {
    s.schedules.get("app", name).unwrap().stats.fires()
}

fn ready_in(s: &ServerState, tube: &str) -> usize {
    s.tubes.get(tube).map_or(0, |t| t.ready.len())
}

fn mins(n: i64) -> jiff::SignedDuration {
    jiff::SignedDuration::from_mins(n)
}

fn secs(n: i64) -> jiff::SignedDuration {
    jiff::SignedDuration::from_secs(n)
}

#[test]
fn schedule_fires_at_its_tick_and_not_before() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let t = next_fire(&s, "ping");

    s.fire_due_schedules(t - secs(1));
    assert_eq!(ready_in(&s, "jobs"), 0, "a sync fires nothing, and nor does an early tick");

    s.fire_due_schedules(t);
    assert_eq!(ready_in(&s, "jobs"), 1);
    assert_eq!(next_fire(&s, "ping"), t + mins(1));
    let e = s.schedules.get("app", "ping").unwrap();
    assert_eq!(e.last_fire, Some(t));
    assert_eq!(e.stats.last_result.as_deref(), Some("INSERTED 1"));

    // The same tick again fires nothing more.
    s.fire_due_schedules(t);
    assert_eq!(ready_in(&s, "jobs"), 1);
}

#[test]
fn missed_ticks_fire_once() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let t = next_fire(&s, "ping");

    // Ten ticks passed unfired: one fire, then the schedule carries on.
    s.fire_due_schedules(t + mins(10) + secs(30));
    assert_eq!(fired(&s, "ping"), 1);
    assert_eq!(next_fire(&s, "ping"), t + mins(11));
}

#[test]
fn catchup_zero_never_fires_a_missed_tick() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("ping", "catchup:0 every 60s")]);
    let t = next_fire(&s, "ping");

    s.fire_due_schedules(t + mins(10) + secs(30));
    assert_eq!(fired(&s, "ping"), 0);
    assert_eq!(next_fire(&s, "ping"), t + mins(11));

    // A tick fired a little late is on time, not missed.
    s.fire_due_schedules(t + mins(11) + secs(2));
    assert_eq!(fired(&s, "ping"), 1);
}

#[test]
fn catchup_bound_fires_only_a_recent_missed_tick() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("ping", "catchup:30 every 60s")]);
    let t = next_fire(&s, "ping");

    // The latest missed tick, t+10m, is 40 s old: past the bound.
    s.fire_due_schedules(t + mins(10) + secs(40));
    assert_eq!(fired(&s, "ping"), 0);

    // The latest missed tick, t+12m, is 20 s old: within it.
    s.fire_due_schedules(t + mins(12) + secs(20));
    assert_eq!(fired(&s, "ping"), 1);
}

#[test]
fn a_backward_clock_step_fires_nothing_again() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let t = next_fire(&s, "ping");

    s.fire_due_schedules(t + mins(10));
    assert_eq!(fired(&s, "ping"), 1);
    for back in [mins(5), mins(1), secs(1)] {
        s.fire_due_schedules(t + mins(10) - back);
    }
    assert_eq!(fired(&s, "ping"), 1);
}

#[test]
fn schedule_fire_records_dedups_and_buried_streak() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("rollup", "idp:rollup every 60s")]);
    let t = next_fire(&s, "rollup");

    s.fire_due_schedules(t);
    s.fire_due_schedules(t + mins(1));
    {
        let st = &s.schedules.get("app", "rollup").unwrap().stats;
        assert_eq!((st.inserted, st.deduped.ready), (1, 1));
        assert_eq!(st.last_result.as_deref(), Some("INSERTED 1 READY"));
    }

    // The job buries; every fire after that queues nothing.
    s.handle_command(c, Command::Watch { tube: "jobs".into(), weight: 1 }, None);
    assert!(matches!(s.handle_command(c, Command::Reserve, None), Response::Reserved { id: 1, .. }));
    assert_eq!(s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None), Response::Buried);
    s.fire_due_schedules(t + mins(2));
    s.fire_due_schedules(t + mins(3));
    let st = &s.schedules.get("app", "rollup").unwrap().stats;
    assert_eq!(st.deduped.buried, 2);
    assert_eq!(st.buried_streak, 2);
    assert_eq!(st.last_result.as_deref(), Some("INSERTED 1 BURIED"));
    assert_eq!(s.tubes["jobs"].stat.idp_dedups.buried, 2, "counted on the tube too");
}

/// The definitions file's inode: each rewrite renames a new file over it.
fn schedules_inode(dir: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(dir.join("schedules")).unwrap().ino()
}

#[test]
fn a_schedule_sync_that_changes_nothing_does_not_rewrite_the_file() {
    // Every commit rewrote and fsynced the whole file on the engine task,
    // stalling every client for as long as that took.
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_state();
    let c = register(&mut s);
    let (files, _) = crate::schedule::store::ScheduleFiles::open(dir.path()).unwrap();
    s.schedule_files = Some(files);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let written = schedules_inode(dir.path());

    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    assert_eq!(schedules_inode(dir.path()), written);
}

#[test]
fn a_schedule_sync_that_only_respells_a_spec_is_saved() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_state();
    let c = register(&mut s);
    let (files, _) = crate::schedule::store::ScheduleFiles::open(dir.path()).unwrap();
    s.schedule_files = Some(files);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);

    sync_schedules(&mut s, c, &[("ping", "every 1m")]);
    assert_eq!(fired(&s, "ping"), 0);
    let (_, restored) = crate::schedule::store::ScheduleFiles::open(dir.path()).unwrap();
    assert_eq!(
        restored.get("app", "ping").unwrap().line.spec_text,
        "every 1m"
    );
}

#[test]
fn a_schedule_sync_that_changes_nothing_saves_one_that_failed_to() {
    // The failed commit's set stands in memory, so the retry finds nothing
    // changed. It still has to reach the disk.
    let dir = tempfile::tempdir().unwrap();
    let defs = dir.path().join("defs");
    let mut s = make_state();
    let c = register(&mut s);
    // Not there yet, so the first commit's write fails.
    let (files, _) = crate::schedule::store::ScheduleFiles::open(&defs).unwrap();
    s.schedule_files = Some(files);

    s.handle_command(
        c,
        Command::ScheduleBegin {
            owner: "app".into(),
        },
        None,
    );
    let line = "schedule-put ping jobs 0 0 60 2 every 60s";
    let cmd = protocol::parse_line(line.as_bytes()).unwrap();
    s.handle_command(c, cmd, Some(b"{}".to_vec()));
    let resp = s.handle_command(c, Command::ScheduleCommit, None);
    assert_eq!(resp, Response::InternalError);

    std::fs::create_dir(&defs).unwrap();
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let (_, restored) = crate::schedule::store::ScheduleFiles::open(&defs).unwrap();
    assert!(restored.get("app", "ping").is_some());
}

#[test]
fn a_tick_due_while_draining_waits_for_the_drain_to_end() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let t = next_fire(&s, "ping");

    s.handle_command(c, Command::Drain, None);
    s.fire_due_schedules(t);
    // Neither fired nor stamped: the tick is still due, and `catchup:`
    // decides what becomes of it when the drain ends or the server restarts.
    assert_eq!(fired(&s, "ping"), 0);
    assert_eq!(s.schedules.get("app", "ping").unwrap().last_fire, None);
    assert_eq!(next_fire(&s, "ping"), t);

    s.handle_command(c, Command::Undrain, None);
    s.fire_due_schedules(t + jiff::SignedDuration::from_secs(2));
    assert_eq!(ready_in(&s, "jobs"), 1);
}

#[test]
fn a_tick_due_after_the_wal_failed_waits_for_the_restart() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let t = next_fire(&s, "ping");

    s.wal_failed = true;
    s.fire_due_schedules(t);
    assert_eq!(fired(&s, "ping"), 0);
    assert_eq!(s.schedules.get("app", "ping").unwrap().last_fire, None);
    assert_eq!(next_fire(&s, "ping"), t);
}

#[test]
fn a_fire_refused_for_memory_is_lost_but_not_recorded_as_a_fire() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = make_state();
    let c = register(&mut s);
    let (files, _) = crate::schedule::store::ScheduleFiles::open(dir.path()).unwrap();
    s.schedule_files = Some(files);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let t = next_fire(&s, "ping");

    s.max_job_bytes = Some(s.memory_in_use());
    s.fire_due_schedules(t);
    let e = s.schedules.get("app", "ping").unwrap();
    assert_eq!((e.stats.rejected, e.stats.inserted), (1, 0));
    assert_eq!(e.stats.last_result.as_deref(), Some("OUT_OF_MEMORY"));
    // Lost like any refused put: the next attempt is the next tick.
    assert_eq!(next_fire(&s, "ping"), t + mins(1));
    // But no job was queued, so last-fire doesn't move — in memory or on
    // disk — and an alert on its staleness still sees the gap.
    assert_eq!(e.last_fire, None);
    let (_, restored) = crate::schedule::store::ScheduleFiles::open(dir.path()).unwrap();
    assert_eq!(restored.get("app", "ping").unwrap().last_fire, None);
}

#[test]
fn a_fire_whose_body_store_write_fails_is_not_recorded_as_a_fire() {
    // Refused before anything was queued, like a fire refused for memory.
    // The put answers INTERNAL_ERROR, but nothing reached the WAL to come
    // back at a restart.
    let dir = tempfile::tempdir().unwrap();
    let toast = tempfile::tempdir().unwrap();
    let mut s = make_state();
    let c = register(&mut s);
    let (files, _) = crate::schedule::store::ScheduleFiles::open(dir.path()).unwrap();
    s.schedule_files = Some(files);
    s.body_store = Some(Arc::new(
        crate::body_store::BodyStore::open(toast.path(), 4096).unwrap(),
    ));
    // The first write can't create its segment.
    std::fs::remove_dir_all(toast.path()).unwrap();

    let body = vec![b'x'; crate::job::HEAP_INLINE_MAX + 1];
    let resp = s.handle_command(
        c,
        Command::ScheduleBegin {
            owner: "app".into(),
        },
        None,
    );
    assert_eq!(resp, Response::Syncing("app".into()));
    let line = format!("schedule-put ping jobs 0 0 60 {} every 60s", body.len());
    let cmd = protocol::parse_line(line.as_bytes()).unwrap();
    assert_eq!(s.handle_command(c, cmd, Some(body)), Response::Staged);
    let resp = s.handle_command(c, Command::ScheduleCommit, None);
    assert!(matches!(resp, Response::Synced(_)), "got {resp:?}");
    let t = next_fire(&s, "ping");

    s.fire_due_schedules(t);
    assert!(!s.wal_failed);
    let e = s.schedules.get("app", "ping").unwrap();
    assert_eq!(e.stats.last_result.as_deref(), Some("INTERNAL_ERROR"));
    assert_eq!(ready_in(&s, "jobs"), 0);
    assert_eq!(e.last_fire, None);
    let (_, restored) = crate::schedule::store::ScheduleFiles::open(dir.path()).unwrap();
    assert_eq!(restored.get("app", "ping").unwrap().last_fire, None);
}

#[test]
fn of_a_tick_s_fires_only_the_one_that_failed_the_wal_is_recorded() {
    // Its record may be on disk and come back at a restart, so it counts
    // as fired. The fires after it are refused before anything is
    // written, like a tick due once the WAL has failed.
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = tempfile::tempdir().unwrap();
    let mut s = make_wal_state(wal_dir.path(), Duration::from_millis(100));
    let c = register(&mut s);
    let (files, _) = crate::schedule::store::ScheduleFiles::open(dir.path()).unwrap();
    s.schedule_files = Some(files);
    sync_schedules(&mut s, c, &[("ping", "every 60s"), ("pong", "every 60s")]);
    let t = next_fire(&s, "ping").max(next_fire(&s, "pong"));

    s.wal.as_mut().unwrap().inject_io_failure();
    s.fire_due_schedules(t);
    assert!(s.wal_failed);
    let last_fire = |s: &crate::schedule::Schedules, name| s.get("app", name).unwrap().last_fire;
    assert_eq!(last_fire(&s.schedules, "ping"), Some(t));
    assert_eq!(last_fire(&s.schedules, "pong"), None);
    let (_, restored) = crate::schedule::store::ScheduleFiles::open(dir.path()).unwrap();
    assert!(last_fire(&restored, "ping").is_some());
    assert_eq!(last_fire(&restored, "pong"), None);
}

#[test]
fn a_fire_that_cannot_be_stamped_is_not_put() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let t = next_fire(&s, "ping");

    // A directory that isn't there: every write fails.
    let gone = std::env::temp_dir().join("tuber-test-no-such-dir/nested");
    let (files, _) = crate::schedule::store::ScheduleFiles::open(&gone).unwrap();
    s.schedule_files = Some(files);

    s.fire_due_schedules(t);
    assert_eq!(ready_in(&s, "jobs"), 0, "at most once: unstamped means unfired");
    let e = s.schedules.get("app", "ping").unwrap();
    assert_eq!(e.last_fire, None);
    assert_eq!((e.stats.rejected, e.stats.inserted), (1, 0));
    assert_eq!(e.stats.last_result.as_deref(), Some("INTERNAL_ERROR"));
}

#[test]
fn a_fire_stamped_by_a_clock_far_ahead_does_not_silence_the_entry() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let t = next_fire(&s, "ping");

    // It fires while the clock reads a month ahead, then the clock is put
    // right. Waiting for the clock to pass the bogus last fire would
    // silence the entry for a month: it resumes from now instead.
    s.fire_due_schedules(t + jiff::SignedDuration::from_hours(24 * 30));
    assert_eq!(fired(&s, "ping"), 1);
    s.fire_due_schedules(t + mins(1));
    assert_eq!(next_fire(&s, "ping"), t + mins(2));
    s.fire_due_schedules(t + mins(2));
    assert_eq!(fired(&s, "ping"), 2);
}

#[test]
fn a_sync_under_a_clock_far_ahead_does_not_silence_the_entry() {
    let mut s = make_state();
    let c = register(&mut s);
    sync_schedules(&mut s, c, &[("ping", "every 60s")]);
    let t = next_fire(&s, "ping");
    // As restored from a sync committed while the clock read a month ahead.
    let ahead = t + jiff::SignedDuration::from_hours(24 * 30);
    let e = s.schedules.get_mut("app", "ping").unwrap();
    e.synced_at = ahead;
    e.next_fire = e.line.spec.next_after(ahead);

    s.fire_due_schedules(t);
    assert_eq!(next_fire(&s, "ping"), t + mins(1));
}
