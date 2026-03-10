# Protocol Reference

beanstalkd uses a text-based protocol over TCP. Commands and responses are
terminated by `\r\n`. Job bodies are raw bytes of the declared length.

## Producer Commands

### put \<pri\> \<delay\> \<ttr\> \<bytes\>\r\n\<body\>\r\n

Insert a job. Body must be exactly `<bytes>` bytes followed by `\r\n`.

Responses:
- `INSERTED <id>\r\n`
- `BURIED <id>\r\n` (out of memory)
- `EXPECTED_CRLF\r\n` (body not terminated correctly)
- `JOB_TOO_BIG\r\n`
- `DRAINING\r\n`

### use \<tube\>\r\n

Switch the connection's use tube.

Response: `USING <tube>\r\n`

## Worker Commands

### reserve\r\n / reserve-with-timeout \<seconds\>\r\n

Reserve a job from watched tubes. Blocks until a job is available or timeout.

Responses:
- `RESERVED <id> <bytes>\r\n<body>\r\n`
- `TIMED_OUT\r\n`
- `DEADLINE_SOON\r\n`

### reserve-job \<id\>\r\n

Reserve a specific job by ID (regardless of tube).

Responses:
- `RESERVED <id> <bytes>\r\n<body>\r\n`
- `NOT_FOUND\r\n`

### delete \<id\>\r\n

Delete a job.

Response: `DELETED\r\n` or `NOT_FOUND\r\n`

### release \<id\> \<pri\> \<delay\>\r\n

Release a reserved job back to ready (or delayed).

Response: `RELEASED\r\n`, `BURIED\r\n`, or `NOT_FOUND\r\n`

### bury \<id\> \<pri\>\r\n

Bury a reserved job.

Response: `BURIED\r\n` or `NOT_FOUND\r\n`

### touch \<id\>\r\n

Reset the TTR timer on a reserved job.

Response: `TOUCHED\r\n` or `NOT_FOUND\r\n`

### watch \<tube\> [weight]\r\n

Add a tube to the watch set. Optional weight for weighted reserve mode.

Response: `WATCHING <count>\r\n`

### ignore \<tube\>\r\n

Remove a tube from the watch set.

Response: `WATCHING <count>\r\n` or `NOT_IGNORED\r\n`

### reserve-mode \<mode\>\r\n

Set reserve mode to `fifo` or `weighted`.

Response: `USING <mode>\r\n` or `BAD_FORMAT\r\n`

## Other Commands

### peek \<id\>\r\n / peek-ready\r\n / peek-delayed\r\n / peek-buried\r\n

Response: `FOUND <id> <bytes>\r\n<body>\r\n` or `NOT_FOUND\r\n`

### kick \<bound\>\r\n

Kick up to `<bound>` buried/delayed jobs in the use tube.

Response: `KICKED <count>\r\n`

### kick-job \<id\>\r\n

Kick a specific job.

Response: `KICKED\r\n` or `NOT_FOUND\r\n`

### stats\r\n / stats-job \<id\>\r\n / stats-tube \<tube\>\r\n

Response: `OK <bytes>\r\n<yaml>\r\n`

### list-tubes\r\n / list-tube-used\r\n / list-tubes-watched\r\n

Response: `OK <bytes>\r\n<yaml>\r\n` or `USING <tube>\r\n`

### pause-tube \<tube> \<delay\>\r\n

Pause a tube for `<delay>` seconds.

Response: `PAUSED\r\n` or `NOT_FOUND\r\n`

### quit\r\n

Close the connection.

## Constants

| Constant | Value |
|---|---|
| Default port | 11300 |
| Max tube name length | 200 chars |
| Tube name chars | `A-Za-z0-9` `-+/;.$_()` |
| Default max job size | 65535 bytes |
| Max possible job size | 1GB |
| Urgent priority threshold | < 1024 |
| Default TTR | 1 second |
| Priority range | 0 (most urgent) to 4294967295 |
