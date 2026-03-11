# Test Plan

The C beanstalkd has 109 test references across 5 test files. We aim for at
least the same coverage, organized by Rust module.

**Current status: 127 tests passing (71 unit + 56 integration)**

## Test Structure

```
src/
  heap.rs          # inline unit tests (#[cfg(test)] mod tests)     -- DONE (6 tests)
  job.rs           # inline unit tests                              -- DONE (8 tests)
  tube.rs          # inline unit tests                              -- DONE (9 tests)
  protocol.rs      # inline unit tests for parsing/formatting       -- DONE (25 tests)
  server.rs        # inline unit tests for engine logic             -- DONE (16 tests)
                   # (subtotal: 64 unit tests; cargo reports 71
                   #  due to additional tests beyond C reference)
tests/
  integration.rs   # full TCP integration tests                     -- DONE (56 tests)
  binlog.rs        # WAL persistence tests (Phase 4)                -- NOT STARTED
```

## Unit Tests by Module

### heap.rs (6 tests -- matches C testheap.c) -- DONE

| C test | Rust test | Status | Description |
|--------|-----------|--------|-------------|
| `cttest_heap_insert_one` | `test_heap_insert_one` | DONE | Insert single element, verify peek |
| `cttest_heap_insert_and_remove_one` | `test_heap_insert_and_remove_one` | DONE | Insert then remove, heap is empty |
| `cttest_heap_priority` | `test_heap_priority` | DONE | Multiple priorities come out in order |
| `cttest_heap_fifo_property` | `test_heap_fifo_property` | DONE | Same priority preserves insertion order (by ID) |
| `cttest_heap_many_jobs` | `test_heap_many_jobs` | DONE | Insert many, verify all removed in order |
| `cttest_heap_remove_k` | `test_heap_remove_by_id` | DONE | Remove element from middle of heap |

### job.rs (8 tests -- matches C testjobs.c) -- DONE

| C test | Rust test | Status | Description |
|--------|-----------|--------|-------------|
| `cttest_job_creation` | `test_job_creation` | DONE | Create job with correct fields |
| `cttest_job_cmp_pris` | `test_job_cmp_priorities` | DONE | Priority comparison ordering |
| `cttest_job_cmp_ids` | `test_job_cmp_ids` | DONE | ID tiebreaker when same priority |
| `cttest_job_large_pris` | `test_job_large_priorities` | DONE | Edge case priority values |
| `cttest_job_hash_free` | `test_job_hash_free` | DONE | Job removed from hashmap on free |
| `cttest_job_hash_free_next` | `test_job_hash_free_chain` | DONE | Chained jobs in same bucket freed correctly |
| `cttest_job_all_jobs_used` | `test_job_count` | DONE | Track job count correctly |
| `cttest_job_100_000_jobs` | `test_job_100k_jobs` | DONE | Stress test: 100k jobs created and freed |

### tube.rs (9 tests -- matches C testms.c, adapted for tubes) -- DONE

The C `testms.c` tests the `Ms` (multiset) data structure used for tube lists
and watch sets. In Rust we use `Vec`/`HashMap` so these tests adapt to our
container types.

| C test | Rust test | Status | Description |
|--------|-----------|--------|-------------|
| `cttest_ms_append` | `test_watch_set_add` | DONE | Add tube to watch set |
| `cttest_ms_remove` | `test_watch_set_remove` | DONE | Remove tube from watch set |
| `cttest_ms_contains` | `test_watch_set_contains` | DONE | Check tube membership |
| `cttest_ms_clear_empty` | `test_watch_set_clear` | DONE | Clear empty set is safe |
| `cttest_ms_take` | `test_watch_set_take` | DONE | Take (round-robin) from set |
| `cttest_ms_take_sequence` | `test_watch_set_take_sequence` | DONE | Take cycles through all elements |
| -- | `test_tube_new` | DONE | Tube creation |
| -- | `test_tube_ready_heap` | DONE | Tube ready heap operations |
| -- | `test_tube_pause` | DONE | Tube pause/unpause |

### protocol.rs (25 parsing tests -- new, no direct C equivalent) -- DONE

| Rust test | Status | Description |
|-----------|--------|-------------|
| `test_parse_put` | DONE | Parse `put 0 0 10 5\r\n` |
| `test_parse_reserve` | DONE | Parse `reserve\r\n` |
| `test_parse_reserve_with_timeout` | DONE | Parse `reserve-with-timeout 30\r\n` |
| `test_parse_reserve_job` | DONE | Parse `reserve-job 123\r\n` |
| `test_parse_delete` | DONE | Parse `delete 123\r\n` |
| `test_parse_release` | DONE | Parse `release 123 0 0\r\n` |
| `test_parse_bury` | DONE | Parse `bury 123 0\r\n` |
| `test_parse_touch` | DONE | Parse `touch 123\r\n` |
| `test_parse_watch` | DONE | Parse `watch foo\r\n` |
| `test_parse_watch_with_weight` | DONE | Parse `watch foo 5\r\n` |
| `test_parse_watch_weight_zero_rejected` | DONE | Weight 0 rejected |
| `test_parse_watch_weight_too_large` | DONE | Weight > MAX rejected |
| `test_parse_ignore` | DONE | Parse `ignore foo\r\n` |
| `test_parse_use` | DONE | Parse `use foo\r\n` |
| `test_parse_kick` | DONE | Parse `kick 10\r\n` |
| `test_parse_kick_job` | DONE | Parse `kick-job 123\r\n` |
| `test_parse_stats` | DONE | Parse `stats\r\n` |
| `test_parse_stats_job` | DONE | Parse `stats-job 123\r\n` |
| `test_parse_stats_tube` | DONE | Parse `stats-tube foo\r\n` |
| `test_parse_list_tubes` | DONE | Parse `list-tubes\r\n` |
| `test_parse_list_tube_used` | DONE | Parse `list-tube-used\r\n` |
| `test_parse_list_tubes_watched` | DONE | Parse `list-tubes-watched\r\n` |
| `test_parse_pause_tube` | DONE | Parse `pause-tube foo 60\r\n` |
| `test_parse_reserve_mode` | DONE | Parse `reserve-mode weighted\r\n` |
| `test_parse_quit` | DONE | Parse `quit\r\n` |
| `test_parse_unknown` | DONE | Unknown command returns error |
| `test_parse_bad_format` | DONE | Malformed arguments return error |
| `test_tube_name_validation` | DONE | Tube names with valid/invalid chars |
| `test_tube_name_max_length` | DONE | 200-char tube name OK, 201 rejected |
| `test_response_serialize` | DONE | Response serialization |

### server.rs (16 engine logic tests) -- DONE

| Rust test | Status | Description |
|-----------|--------|-------------|
| `test_put_and_reserve` | DONE | Basic put/reserve cycle |
| `test_priority_ordering` | DONE | Lower priority number reserved first |
| `test_fifo_same_priority` | DONE | Same priority -> FIFO by job ID |
| `test_bury_and_kick` | DONE | Bury then kick returns job to ready |
| `test_kick_job_buried` | DONE | Kick a specific buried job by ID |
| `test_delete_ready` | DONE | Delete a ready job |
| `test_release_to_ready` | DONE | Release with delay=0 -> ready |
| `test_touch` | DONE | Touch extends deadline |
| `test_multi_tube` | DONE | Put to one tube, reserve from another |
| `test_pause_tube` | DONE | Paused tube blocks reserve |
| `test_drain_mode` | DONE | Drain mode rejects puts |
| `test_close_releases_jobs` | DONE | Disconnect releases reserved jobs |
| `test_reserve_mode_weighted` | DONE | Weighted mode distributes across tubes |
| `test_watch_and_ignore` | DONE | Watch/ignore tube management |
| `test_stats_job` | DONE | stats-job YAML output |
| `test_list_tubes` | DONE | list-tubes output |

Not yet implemented as unit tests (covered by integration tests instead):
- `test_delayed_job_promotion` -- tested via `test_delayed_to_ready` integration test
- `test_ttr_expiry` -- tested via `test_reserve_ttr_deadline_soon` integration test
- `test_kick_count` -- tested via `test_peek_buried_kick` integration test
- `test_delete_reserved` -- tested via `test_delete_reserved_by_other` integration test
- `test_release_to_delayed` -- tested via `test_reserve_job_delayed` integration test
- `test_unpause_tube` -- tested via `test_unpause_tube` integration test

## Integration Tests (tests/integration.rs) -- DONE (56 tests)

These tests start the actual TCP server and send real protocol commands, matching
the C `testserv.c` tests. They verify wire-compatibility.

### Batch 1: Basic protocol (30 tests) -- DONE

| C test | Rust test | Status | Description |
|--------|-----------|--------|-------------|
| `cttest_unknown_command` | `test_unknown_command` | DONE | Server responds `UNKNOWN_COMMAND\r\n` |
| `cttest_peek_ok` | `test_peek_ok` | DONE | Peek returns FOUND with correct body |
| `cttest_peek_not_found` | `test_peek_not_found` | DONE | Peek nonexistent returns NOT_FOUND |
| `cttest_peek_bad_format` | `test_peek_bad_format` | DONE | Bad peek arg returns BAD_FORMAT |
| `cttest_touch_bad_format` | `test_touch_bad_format` | DONE | Bad touch arg |
| `cttest_touch_not_found` | `test_touch_not_found` | DONE | Touch nonexistent job |
| `cttest_bury_bad_format` | `test_bury_bad_format` | DONE | Bad bury args |
| `cttest_kickjob_bad_format` | `test_kickjob_bad_format` | DONE | Bad kick-job arg |
| `cttest_delete_ready` | `test_delete_ready_job` | DONE | Delete ready job |
| `cttest_delete_bad_format` | `test_delete_bad_format` | DONE | Bad delete arg |
| `cttest_release_bad_format` | `test_release_bad_format` | DONE | Bad release args |
| `cttest_release_not_found` | `test_release_not_found` | DONE | Release nonexistent job |
| `cttest_underscore` | `test_underscore_tube` | DONE | Tube name with underscore |
| `cttest_2cmdpacket` | `test_two_commands_one_packet` | DONE | Two commands in single TCP packet |
| `cttest_too_big` | `test_too_big_job` | DONE | Job body exceeds size limit |
| `cttest_job_size_invalid` | `test_job_size_invalid` | DONE | Negative/bad body size |
| `cttest_negative_delay` | `test_negative_delay` | DONE | Negative delay rejected |
| `cttest_garbage_priority` | `test_garbage_priority` | DONE | Non-numeric priority |
| `cttest_negative_priority` | `test_negative_priority` | DONE | Negative priority rejected |
| `cttest_max_priority` | `test_max_priority` | DONE | Priority = u32::MAX |
| `cttest_too_big_priority` | `test_too_big_priority` | DONE | Priority > u32::MAX |
| `cttest_zero_delay` | `test_zero_delay` | DONE | Delay = 0 goes straight to ready |
| `cttest_list_tube` | `test_list_tubes` | DONE | list-tubes output |
| `cttest_use_tube_long` | `test_use_tube_long_name` | DONE | 200-char tube name |
| `cttest_reserve_mode_fifo` | `test_reserve_mode_fifo` | DONE | Set reserve mode to fifo |
| `cttest_reserve_mode_weighted` | `test_reserve_mode_weighted` | DONE | Set reserve mode to weighted |
| `cttest_reserve_mode_bad_format` | `test_reserve_mode_bad_format` | DONE | Invalid reserve mode |
| `cttest_watch_with_weight` | `test_watch_with_weight` | DONE | watch tube with weight |
| `cttest_watch_weight_default` | `test_watch_weight_default` | DONE | Default weight = 1 |
| `cttest_watch_weight_zero_rejected` | `test_watch_weight_zero` | DONE | Weight 0 rejected |
| `cttest_watch_weight_too_large` | `test_watch_weight_too_large` | DONE | Weight > MAX rejected |

### Batch 2: Job lifecycle (12 tests) -- DONE

| C test | Rust test | Status | Description |
|--------|-----------|--------|-------------|
| `cttest_peek_delayed` | `test_peek_delayed` | DONE | peek-delayed finds delayed job |
| `cttest_peek_buried_kick` | `test_peek_buried_kick` | DONE | Peek buried, then kick it back |
| `cttest_kickjob_buried` | `test_kickjob_buried` | DONE | kick-job on buried job |
| `cttest_kickjob_delayed` | `test_kickjob_delayed` | DONE | kick-job on delayed job |
| `cttest_statsjob_ck_format` | `test_stats_job_format` | DONE | YAML format of stats-job |
| `cttest_stats_tube` | `test_stats_tube_format` | DONE | YAML format of stats-tube |
| `cttest_ttrlarge` | `test_ttr_large` | DONE | Large TTR value |
| `cttest_ttr_small` | `test_ttr_small` | DONE | TTR = 0 (minimum 1 second) |
| `cttest_reserve_job_ready` | `test_reserve_job_ready` | DONE | reserve-job on ready job |
| `cttest_reserve_job_delayed` | `test_reserve_job_delayed` | DONE | reserve-job on delayed job |
| `cttest_reserve_job_buried` | `test_reserve_job_buried` | DONE | reserve-job on buried job |
| `cttest_reserve_job_already_reserved` | `test_reserve_job_already_reserved` | DONE | reserve-job on reserved job |

### Batch 3: Multi-connection & timing (14 tests) -- DONE

| C test | Rust test | Status | Description |
|--------|-----------|--------|-------------|
| `cttest_multi_tube` | `test_multi_tube` | DONE | Use/watch different tubes |
| `cttest_reserve_with_timeout_2conns` | `test_reserve_timeout_two_conns` | DONE | Two connections reserve simultaneously |
| `cttest_close_releases_job` | `test_close_releases_job` | DONE | TCP close releases reserved jobs |
| `cttest_quit_releases_job` | `test_quit_releases_job` | DONE | quit command releases reserved jobs |
| `cttest_delete_reserved_by_other` | `test_delete_reserved_by_other` | DONE | Delete job reserved by another conn |
| `cttest_small_delay` | `test_small_delay` | DONE | Delay = 1 second |
| `cttest_delayed_to_ready` | `test_delayed_to_ready` | DONE | Wait for delayed->ready transition |
| `cttest_pause` | `test_pause_tube` | DONE | Pause tube blocks reserve |
| `cttest_unpause_tube` | `test_unpause_tube` | DONE | Tube unpauses after delay |
| `cttest_reserve_ttr_deadline_soon` | `test_reserve_ttr_deadline_soon` | DONE | DEADLINE_SOON when TTR almost expired |
| `cttest_reserve_job_ttr_deadline_soon` | `test_reserve_job_ttr_deadline_soon` | DONE | Same for reserve-job |
| `cttest_weighted_reserve_empty_tube` | `test_weighted_reserve_empty` | DONE | Weighted reserve with empty tube |
| `cttest_weighted_reserve_distribution` | `test_weighted_reserve_distribution` | DONE | Verify weighted distribution |

### Not yet implemented integration tests

| C test | Rust test | Status | Description |
|--------|-----------|--------|-------------|
| `cttest_too_long_commandline` | `test_too_long_commandline` | NOT DONE | Command over LINE_BUF_SIZE rejected |
| `cttest_put_in_drain` | `test_put_in_drain` | NOT DONE | Put returns `DRAINING\r\n` (requires signal handling) |
| `cttest_job_size_max_plus_1` | `test_job_size_max_plus_1` | NOT DONE | Body size = max+1 (requires sending 1GB+ data) |
| `cttest_omit_time_left` | `test_omit_time_left` | NOT DONE | stats-job time-left field |
| `cttest_longest_command` | `test_longest_command` | NOT DONE | Maximum length command line |

## WAL Tests (tests/binlog.rs -- Phase 4) -- NOT STARTED

| C test | Rust test | Status | Description |
|--------|-----------|--------|-------------|
| `cttest_binlog_empty_exit` | `test_binlog_empty_exit` | NOT DONE | Empty binlog on clean shutdown |
| `cttest_binlog_bury` | `test_binlog_bury` | NOT DONE | Bury persisted to WAL |
| `cttest_binlog_basic` | `test_binlog_basic` | NOT DONE | Basic put/delete cycle in WAL |
| `cttest_binlog_size_limit` | `test_binlog_size_limit` | NOT DONE | WAL file rotation at size limit |
| `cttest_binlog_allocation` | `test_binlog_allocation` | NOT DONE | WAL space reservation |
| `cttest_binlog_read` | `test_binlog_read` | NOT DONE | Read back WAL on startup |
| `cttest_binlog_disk_full` | `test_binlog_disk_full` | NOT DONE | Handle disk full gracefully |
| `cttest_binlog_disk_full_delete` | `test_binlog_disk_full_delete` | NOT DONE | Delete works when disk full |
| `cttest_binlog_v5` | `test_binlog_v5_compat` | NOT DONE | Read legacy v5 binlog format |

## CLI Option Tests (inline in main.rs or separate) -- NOT STARTED

| C test | Rust test | Status | Description |
|--------|-----------|--------|-------------|
| `cttest_allocf` | (N/A -- Rust formatting) | -- | |
| `cttest_opt_none` | `test_default_args` | NOT DONE | Default CLI values |
| `cttest_optminus` | `test_invalid_flag` | NOT DONE | `-` exits with error |
| `cttest_optp` | `test_port_flag` | NOT DONE | `-p1234` |
| `cttest_optl` / `cttest_optlseparate` | `test_listen_flag` | NOT DONE | `-l localhost` |
| `cttest_optz` / `cttest_optz_more_than_max` | `test_max_job_size_flag` | NOT DONE | `-z` with bounds |
| `cttest_opts` | `test_filesize_flag` | NOT DONE | `-s1234` |
| `cttest_optf` / `cttest_optF` | `test_sync_flags` | NOT DONE | `-f` and `-F` |
| `cttest_optu` | `test_user_flag` | NOT DONE | `-u kr` |
| `cttest_optb` | `test_binlog_flag` | NOT DONE | `-b foo` |
| `cttest_optV*` | `test_verbose_flags` | NOT DONE | `-V`, `-VVV` |

## Running Tests

```bash
# All tests (127 total: 71 unit + 56 integration)
cargo test

# Integration tests only (56 tests)
cargo test --test integration

# Specific module tests
cargo test heap::tests
cargo test job::tests
```
