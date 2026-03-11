# Test Plan

The C beanstalkd has 109 test references across 5 test files. We aim for at
least the same coverage, organized by Rust module.

## Test Structure

```
src/
  heap.rs          # inline unit tests (#[cfg(test)] mod tests)
  job.rs           # inline unit tests
  tube.rs          # inline unit tests
  protocol.rs      # inline unit tests for parsing/formatting
  server.rs        # inline unit tests for engine logic
tests/
  integration.rs   # full TCP integration tests (start server, send commands)
  binlog.rs        # WAL persistence tests (Phase 4)
```

## Unit Tests by Module

### heap.rs (6 tests -- matches C testheap.c)

| C test | Rust test | Description |
|--------|-----------|-------------|
| `cttest_heap_insert_one` | `test_heap_insert_one` | Insert single element, verify peek |
| `cttest_heap_insert_and_remove_one` | `test_heap_insert_and_remove_one` | Insert then remove, heap is empty |
| `cttest_heap_priority` | `test_heap_priority` | Multiple priorities come out in order |
| `cttest_heap_fifo_property` | `test_heap_fifo_property` | Same priority preserves insertion order (by ID) |
| `cttest_heap_many_jobs` | `test_heap_many_jobs` | Insert many, verify all removed in order |
| `cttest_heap_remove_k` | `test_heap_remove_by_id` | Remove element from middle of heap |

### job.rs (8 tests -- matches C testjobs.c)

| C test | Rust test | Description |
|--------|-----------|-------------|
| `cttest_job_creation` | `test_job_creation` | Create job with correct fields |
| `cttest_job_cmp_pris` | `test_job_cmp_priorities` | Priority comparison ordering |
| `cttest_job_cmp_ids` | `test_job_cmp_ids` | ID tiebreaker when same priority |
| `cttest_job_large_pris` | `test_job_large_priorities` | Edge case priority values |
| `cttest_job_hash_free` | `test_job_hash_free` | Job removed from hashmap on free |
| `cttest_job_hash_free_next` | `test_job_hash_free_chain` | Chained jobs in same bucket freed correctly |
| `cttest_job_all_jobs_used` | `test_job_count` | Track job count correctly |
| `cttest_job_100_000_jobs` | `test_job_100k_jobs` | Stress test: 100k jobs created and freed |

### tube.rs (6 tests -- matches C testms.c, adapted for tubes)

The C `testms.c` tests the `Ms` (multiset) data structure used for tube lists
and watch sets. In Rust we use `Vec`/`HashMap` so these tests adapt to our
container types.

| C test | Rust test | Description |
|--------|-----------|-------------|
| `cttest_ms_append` | `test_watch_set_add` | Add tube to watch set |
| `cttest_ms_remove` | `test_watch_set_remove` | Remove tube from watch set |
| `cttest_ms_contains` | `test_watch_set_contains` | Check tube membership |
| `cttest_ms_clear_empty` | `test_watch_set_clear` | Clear empty set is safe |
| `cttest_ms_take` | `test_watch_set_take` | Take (round-robin) from set |
| `cttest_ms_take_sequence` | `test_watch_set_take_sequence` | Take cycles through all elements |

### protocol.rs (parsing tests -- new, no direct C equivalent)

| Rust test | Description |
|-----------|-------------|
| `test_parse_put` | Parse `put 0 0 10 5\r\n` |
| `test_parse_reserve` | Parse `reserve\r\n` |
| `test_parse_reserve_with_timeout` | Parse `reserve-with-timeout 30\r\n` |
| `test_parse_reserve_job` | Parse `reserve-job 123\r\n` |
| `test_parse_delete` | Parse `delete 123\r\n` |
| `test_parse_release` | Parse `release 123 0 0\r\n` |
| `test_parse_bury` | Parse `bury 123 0\r\n` |
| `test_parse_touch` | Parse `touch 123\r\n` |
| `test_parse_watch` | Parse `watch foo\r\n` |
| `test_parse_watch_with_weight` | Parse `watch foo 5\r\n` |
| `test_parse_ignore` | Parse `ignore foo\r\n` |
| `test_parse_use` | Parse `use foo\r\n` |
| `test_parse_kick` | Parse `kick 10\r\n` |
| `test_parse_kick_job` | Parse `kick-job 123\r\n` |
| `test_parse_stats` | Parse `stats\r\n` |
| `test_parse_stats_job` | Parse `stats-job 123\r\n` |
| `test_parse_stats_tube` | Parse `stats-tube foo\r\n` |
| `test_parse_list_tubes` | Parse `list-tubes\r\n` |
| `test_parse_pause_tube` | Parse `pause-tube foo 60\r\n` |
| `test_parse_reserve_mode` | Parse `reserve-mode weighted\r\n` |
| `test_parse_quit` | Parse `quit\r\n` |
| `test_parse_unknown` | Unknown command returns error |
| `test_parse_bad_format` | Malformed arguments return error |
| `test_tube_name_validation` | Tube names with valid/invalid chars |
| `test_tube_name_max_length` | 200-char tube name OK, 201 rejected |

### server.rs (engine logic tests)

| Rust test | Description |
|-----------|-------------|
| `test_put_and_reserve` | Basic put/reserve cycle |
| `test_priority_ordering` | Lower priority number reserved first |
| `test_fifo_same_priority` | Same priority -> FIFO by job ID |
| `test_delayed_job_promotion` | Delayed job becomes ready after delay |
| `test_ttr_expiry` | Reserved job returns to ready after TTR |
| `test_bury_and_kick` | Bury then kick returns job to ready |
| `test_kick_count` | Kick N buried jobs |
| `test_kick_job_specific` | Kick a specific job by ID |
| `test_delete_ready` | Delete a ready job |
| `test_delete_reserved` | Delete a reserved job |
| `test_release_to_ready` | Release with delay=0 -> ready |
| `test_release_to_delayed` | Release with delay>0 -> delayed |
| `test_touch_resets_ttr` | Touch extends deadline |
| `test_multi_tube` | Put to one tube, reserve from another |
| `test_pause_tube` | Paused tube blocks reserve |
| `test_unpause_tube` | Tube unpauses after delay |
| `test_drain_mode` | Drain mode rejects puts |
| `test_close_releases_jobs` | Disconnect releases reserved jobs |
| `test_weighted_reserve` | Weighted mode distributes across tubes |

## Integration Tests (tests/integration.rs)

These tests start the actual TCP server and send real protocol commands, matching
the C `testserv.c` tests. They verify wire-compatibility.

| C test | Rust test | Description |
|--------|-----------|-------------|
| `cttest_unknown_command` | `test_unknown_command` | Server responds `UNKNOWN_COMMAND\r\n` |
| `cttest_too_long_commandline` | `test_too_long_commandline` | Command over LINE_BUF_SIZE rejected |
| `cttest_put_in_drain` | `test_put_in_drain` | Put returns `DRAINING\r\n` |
| `cttest_peek_ok` | `test_peek_ok` | Peek returns FOUND with correct body |
| `cttest_peek_not_found` | `test_peek_not_found` | Peek nonexistent returns NOT_FOUND |
| `cttest_peek_bad_format` | `test_peek_bad_format` | Bad peek arg returns BAD_FORMAT |
| `cttest_peek_delayed` | `test_peek_delayed` | peek-delayed finds delayed job |
| `cttest_peek_buried_kick` | `test_peek_buried_kick` | Peek buried, then kick it back |
| `cttest_touch_bad_format` | `test_touch_bad_format` | Bad touch arg |
| `cttest_touch_not_found` | `test_touch_not_found` | Touch nonexistent job |
| `cttest_bury_bad_format` | `test_bury_bad_format` | Bad bury args |
| `cttest_kickjob_bad_format` | `test_kickjob_bad_format` | Bad kick-job arg |
| `cttest_kickjob_buried` | `test_kickjob_buried` | kick-job on buried job |
| `cttest_kickjob_delayed` | `test_kickjob_delayed` | kick-job on delayed job |
| `cttest_pause` | `test_pause` | Pause tube blocks reserve |
| `cttest_underscore` | `test_underscore_tube` | Tube name with underscore |
| `cttest_2cmdpacket` | `test_two_commands_one_packet` | Two commands in single TCP packet |
| `cttest_too_big` | `test_too_big_job` | Job body exceeds size limit |
| `cttest_job_size_invalid` | `test_job_size_invalid` | Negative/bad body size |
| `cttest_job_size_max_plus_1` | `test_job_size_max_plus_1` | Body size = max+1 |
| `cttest_delete_ready` | `test_delete_ready_job` | Delete ready job |
| `cttest_delete_reserved_by_other` | `test_delete_reserved_by_other` | Delete job reserved by another conn |
| `cttest_delete_bad_format` | `test_delete_bad_format` | Bad delete arg |
| `cttest_multi_tube` | `test_multi_tube` | Use/watch different tubes |
| `cttest_negative_delay` | `test_negative_delay` | Negative delay rejected |
| `cttest_garbage_priority` | `test_garbage_priority` | Non-numeric priority |
| `cttest_negative_priority` | `test_negative_priority` | Negative priority treated as large |
| `cttest_max_priority` | `test_max_priority` | Priority = u32::MAX |
| `cttest_too_big_priority` | `test_too_big_priority` | Priority > u32::MAX |
| `cttest_omit_time_left` | `test_omit_time_left` | stats-job time-left field |
| `cttest_small_delay` | `test_small_delay` | Delay = 1 second |
| `cttest_delayed_to_ready` | `test_delayed_to_ready` | Wait for delayed->ready transition |
| `cttest_statsjob_ck_format` | `test_stats_job_format` | YAML format of stats-job |
| `cttest_stats_tube` | `test_stats_tube_format` | YAML format of stats-tube |
| `cttest_ttrlarge` | `test_ttr_large` | Large TTR value |
| `cttest_ttr_small` | `test_ttr_small` | TTR = 0 (minimum 1 second) |
| `cttest_zero_delay` | `test_zero_delay` | Delay = 0 goes straight to ready |
| `cttest_reserve_with_timeout_2conns` | `test_reserve_timeout_two_conns` | Two connections reserve simultaneously |
| `cttest_reserve_ttr_deadline_soon` | `test_reserve_ttr_deadline_soon` | DEADLINE_SOON when TTR almost expired |
| `cttest_reserve_job_ttr_deadline_soon` | `test_reserve_job_ttr_deadline_soon` | Same for reserve-job |
| `cttest_reserve_job_already_reserved` | `test_reserve_job_already_reserved` | reserve-job on reserved job |
| `cttest_reserve_job_ready` | `test_reserve_job_ready` | reserve-job on ready job |
| `cttest_reserve_job_delayed` | `test_reserve_job_delayed` | reserve-job on delayed job |
| `cttest_reserve_job_buried` | `test_reserve_job_buried` | reserve-job on buried job |
| `cttest_release_bad_format` | `test_release_bad_format` | Bad release args |
| `cttest_release_not_found` | `test_release_not_found` | Release nonexistent job |
| `cttest_close_releases_job` | `test_close_releases_job` | TCP close releases reserved jobs |
| `cttest_quit_releases_job` | `test_quit_releases_job` | quit command releases reserved jobs |
| `cttest_unpause_tube` | `test_unpause_tube` | Tube unpauses after delay |
| `cttest_list_tube` | `test_list_tubes` | list-tubes output |
| `cttest_use_tube_long` | `test_use_tube_long_name` | 200-char tube name |
| `cttest_longest_command` | `test_longest_command` | Maximum length command line |
| `cttest_reserve_mode_fifo` | `test_reserve_mode_fifo` | Set reserve mode to fifo |
| `cttest_reserve_mode_weighted` | `test_reserve_mode_weighted` | Set reserve mode to weighted |
| `cttest_reserve_mode_bad_format` | `test_reserve_mode_bad_format` | Invalid reserve mode |
| `cttest_watch_with_weight` | `test_watch_with_weight` | watch tube with weight |
| `cttest_watch_weight_default` | `test_watch_weight_default` | Default weight = 1 |
| `cttest_watch_weight_zero_rejected` | `test_watch_weight_zero` | Weight 0 rejected |
| `cttest_watch_weight_too_large` | `test_watch_weight_too_large` | Weight > MAX rejected |
| `cttest_weighted_reserve_empty_tube` | `test_weighted_reserve_empty` | Weighted reserve with empty tube |
| `cttest_weighted_reserve_distribution` | `test_weighted_reserve_distribution` | Verify weighted distribution |

## WAL Tests (tests/binlog.rs -- Phase 4)

| C test | Rust test | Description |
|--------|-----------|-------------|
| `cttest_binlog_empty_exit` | `test_binlog_empty_exit` | Empty binlog on clean shutdown |
| `cttest_binlog_bury` | `test_binlog_bury` | Bury persisted to WAL |
| `cttest_binlog_basic` | `test_binlog_basic` | Basic put/delete cycle in WAL |
| `cttest_binlog_size_limit` | `test_binlog_size_limit` | WAL file rotation at size limit |
| `cttest_binlog_allocation` | `test_binlog_allocation` | WAL space reservation |
| `cttest_binlog_read` | `test_binlog_read` | Read back WAL on startup |
| `cttest_binlog_disk_full` | `test_binlog_disk_full` | Handle disk full gracefully |
| `cttest_binlog_disk_full_delete` | `test_binlog_disk_full_delete` | Delete works when disk full |
| `cttest_binlog_v5` | `test_binlog_v5_compat` | Read legacy v5 binlog format |

## CLI Option Tests (inline in main.rs or separate)

| C test | Rust test | Description |
|--------|-----------|-------------|
| `cttest_allocf` | (N/A -- Rust formatting) | |
| `cttest_opt_none` | `test_default_args` | Default CLI values |
| `cttest_optminus` | `test_invalid_flag` | `-` exits with error |
| `cttest_optp` | `test_port_flag` | `-p1234` |
| `cttest_optl` / `cttest_optlseparate` | `test_listen_flag` | `-l localhost` |
| `cttest_optz` / `cttest_optz_more_than_max` | `test_max_job_size_flag` | `-z` with bounds |
| `cttest_opts` | `test_filesize_flag` | `-s1234` |
| `cttest_optf` / `cttest_optF` | `test_sync_flags` | `-f` and `-F` |
| `cttest_optu` | `test_user_flag` | `-u kr` |
| `cttest_optb` | `test_binlog_flag` | `-b foo` |
| `cttest_optV*` | `test_verbose_flags` | `-V`, `-VVV` |

## Running Tests

```bash
# All unit tests
cargo test

# Integration tests only
cargo test --test integration

# Specific module tests
cargo test heap::tests
cargo test job::tests
```
