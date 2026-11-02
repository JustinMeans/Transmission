# Transmission Live Protocol, version 1

Live data over one WebSocket. This is the contract between servers and clients that ship independently, so it evolves by the rules in [Evolution](#evolution) and is pinned by the golden fixtures in `Tests/TransmissionLiveTests/Fixtures/frames-v1.txt`.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## Scope

The protocol carries topics: named streams with a current state, a monotonic sequence and a bounded history. Nothing in it depends on distributed actors.

Not everything is pushed. Sources that can only be sampled are sampled once on the server, on demand, and published into topics. Historic ranges stay on HTTP.

## Connection

1. The client opens a WebSocket on the live route (default `/v3/live`) with `Sec-WebSocket-Protocol: tx-live.1` and, when it has one, `Authorization: Bearer <token>`.
2. The server authenticates before upgrading. It MUST answer 401 to a bad token and 400 to a client that does not offer `tx-live.1`.
3. The client sends `HELLO` as its first frame.
4. The server answers `WELCOME`, or `ERROR` then close `4001` when no protocol version overlaps. Any other first frame is closed with `4004`.
5. The client sends `SUB` for each subscription.

`clientID` is stable per install and is not a socket id. A new connection with the same authenticated subject and `clientID` supersedes the old one, which is closed with `4003`. Comparison is by connection instance, so a late close from the old socket cannot affect the new one.

All frames are binary WebSocket messages. One message carries exactly one frame.

### Close codes

| Code | Name | Client behaviour |
|---|---|---|
| 1000 | Normal | Stop if the client initiated it, otherwise reconnect |
| 4000 | Slow consumer | Reconnect and resume |
| 4001 | Update required | Stop. Tell the user to update |
| 4002 | Auth failed | Refresh the token, then reconnect with backoff |
| 4003 | Superseded | Stop. Another connection for this client took over |
| 4004 | Protocol error | Reconnect with backoff |
| 4005 | Dead peer | Reconnect with backoff |
| 4006 | Auth expired | Refresh the token, then reconnect with backoff |

## Encoding

- `u8`: one byte.
- `varint`: unsigned LEB128, at most 10 bytes, at most 64 bits.
- `string`: `varint` byte length, then UTF-8.
- `bytes`: `varint` byte length, then the bytes.

Every frame starts with a header:

```
type:u8  flags:u8  subID:varint  ...type-specific fields...
```

`subID` is chosen by the client, is unique within a connection and starts at 1. It is 0 on frames that concern the whole connection.

Unknown `flags` bits MUST be ignored. Lengths and counts MUST be bounds-checked against the bytes remaining; a frame that fails this is malformed and the connection is closed with `4004`.

### Frames

Direction: C client to server, S server to client.

| Type | Name | Dir | Fields after the header |
|---|---|---|---|
| 0x01 | HELLO | C | protoMin:varint, protoMax:varint, build:string, clientID:string |
| 0x02 | WELCOME | S | proto:varint, epoch:varint, serverTimeMs:varint, heartbeatMs:varint, maxFrame:varint, count:varint, then count of { name:string, schemaMin:varint, schemaMax:varint, kind:u8 } |
| 0x03 | SUB | C | topic:string, count:varint, then count of { key:string, value:string } sorted by key, schema:varint, maxMilliHz:varint. If flag 0x01 (RESUME): epoch:varint, seq:varint |
| 0x04 | UNSUB | C | none |
| 0x05 | PAUSE | C | none |
| 0x06 | RESUME | C | none |
| 0x07 | SUBACK | S | epoch:varint, headSeq:varint, schema:varint, mode:u8 (0 snapshot, 1 replay) |
| 0x10 | SNAPSHOT | S | epoch:varint, seq:varint, schema:varint, part:varint, payload:bytes. Flag 0x01 (LAST) marks the final part |
| 0x11 | DELTA | S | seq:varint, sampleTimeMs:varint. If flag 0x01 (SPAN): span:varint. Then payload:bytes |
| 0x12 | GAP | S | fromSeq:varint, toSeq:varint, droppedCount:varint, reason:u8 (0 overflow, 1 outside ring) |
| 0x13 | ACK | C | seq:varint |
| 0x20 | HB | both | serverTimeMs:varint |
| 0x21 | AUTH_REFRESH | C | token:string |
| 0x22 | AUTH_EXPIRING | S | secondsLeft:varint |
| 0x30 | ERROR | S | code:varint, retryable:u8, message:string |
| 0x31 | CALL | C | alias:string, callID:varint, payload:bytes |
| 0x32 | REPLY | S | callID:varint, payload:bytes. Flag 0x01 (ERROR) marks a failure |

`maxMilliHz` is the requested rate in thousandths of a hertz, so 0.2 Hz is 200. 0 means unlimited.

`ERROR` codes: 1 unknown topic, 2 unsupported schema, 3 forbidden, 4 update required, 5 too many subscriptions, 6 unknown alias, 7 malformed, 8 auth failed. `subID` is 0 when the error concerns the connection. Unknown codes MUST be treated as non-specific errors.

`CALL` and `REPLY` are specified and implemented in the codec. This version of the server answers every `CALL` with a failed `REPLY`, because no alias table exists yet.

### Differences from the audit's sketch

The design in `transmission-audit.md` section 10.2 changed in four places during specification.

- **`SUBACK` is new.** After a resume with nothing to replay, a quiet topic would send nothing, and the client could never tell "caught up" from "still waiting". `SUBACK` gives it the head sequence to compare with.
- **`DELTA` gains an optional `span`.** State and series topics coalesce, so one delta can cover several sequences. `span` says how many; the client rule is `seq - span == lastSeq`, and without the flag `span` is 1, which is the sketch's `lastSeq + 1`.
- **`heartbeatMs` replaces `heartbeatSec`.** Milliseconds cost nothing on the wire and let tests run in real time.
- **`maxMilliHz` replaces `maxHz`.** Varints have no fractions, and slow topics need rates below 1 Hz.

## Topics

A topic is identified by `name`, `params` and `schema`. Each instance has:

- `epoch`: random, chosen when the instance starts. A restart changes it.
- `seq`: per instance, monotonic, starting at 1, assigned once at the source.
- a ring of recent deltas, bounded by count, bytes and age.
- a current state.

| Kind | Value | Overflow | Notes |
|---|---|---|---|
| State | 0 | Merge; intermediates vanish | Latest wins |
| Series | 1 | Downsample | `maxMilliHz` applies |
| Log | 2 | Drop oldest, send `GAP` | Clients show the loss |
| Event | 3 | Same as Log | A notification only. Persist first; clients fetch by cursor |

`maxMilliHz` applies to State and Series. Log and Event are never rate limited.

## Subscribing

For every `SUB`, the server sends `SUBACK` first, then data. A `SUB` for a `subID` already in use replaces that subscription.

- **Fresh `SUB`**, or a resume whose epoch differs or that is ahead of the head: `SUBACK` mode 0, then `SNAPSHOT` parts in order.
- **Resume, same epoch, in the ring:** `SUBACK` mode 1, then `DELTA`s from `seq + 1`.
- **Resume, same epoch, behind the ring, Log or Event:** `SUBACK` mode 1, then `GAP`, then `DELTA`s from the ring's tail.
- **Resume, same epoch, behind the ring, State or Series:** the server MAY send a coalesced `DELTA` or a `SNAPSHOT`.

A `SNAPSHOT` is authoritative: it replaces state and sets `lastSeq`. A snapshot larger than `maxFrame` is split into parts; the client concatenates the payloads and decodes on `LAST`.

### Client rules

Let `span` be 1 when the flag is absent.

1. `seq <= lastSeq`: a duplicate. Ignore it.
2. `seq - span == lastSeq`: apply it.
3. Otherwise: a hole. Ignore further deltas and send `SUB` with the last good cursor. It is `.resyncing` until `SUBACK` and catch-up.
4. On `GAP` in a Log or Event topic: set `lastSeq = toSeq` and show the user `droppedCount`. Never hide it.
5. On `GAP` in a State or Series topic: treat as a hole.
6. A subscription is live once it has a state and `lastSeq >= headSeq` from its most recent `SUBACK`.

Subscriptions belong to the client, not the socket. After a reconnect the client re-issues every `SUB` with its cursor.

### Pause

`PAUSE` keeps the subscription and its cursor and stops delivery. `RESUME` continues from the cursor under the normal overflow rules.

On app background the client SHOULD send `PAUSE` for all subscriptions, close with 1000 and keep its cursors. On foreground or a network path change it SHOULD reconnect immediately, bypassing backoff.

## Heartbeat

The server sends `HB` every `heartbeatMs` (default 25,000). The client echoes it. Any inbound frame counts as life. A side that hears nothing for two intervals treats the peer as dead: the server closes with `4005`, the client reconnects.

## Flow control

`ACK` is advisory: the server records it as a lag signal and never waits on it.

Each connection has one writer and one bounded mailbox per subscription. A mailbox is a cursor into the shared ring, so a slow client holds no copy of the data. A publisher never waits for a client.

Hard limits, both configurable: a single write stalled for 30 seconds, or 4 MiB queued behind a subscription's cursors. Either closes with `4000`. The client resumes and gets caught up, so the event is local and self-healing.

## Payloads and evolution

### Evolution

- Receivers MUST ignore unknown frame types.
- Receivers MUST ignore unknown trailing bytes inside a known frame.
- Senders append fields to the end of a frame. An existing field never moves or changes meaning.
- A breaking wire change takes a new protocol version and is negotiated by `HELLO`.

### Payloads

Payloads are JSON, versioned by the topic's `schema`.

- Fields are only added, and are optional or defaulted.
- Unknown enum cases decode to `.unknown`.
- A breaking payload change takes a new `schema`, and the server publishes both during the overlap. Each schema is its own topic instance with its own sequence.

## Authentication

The server authorizes every `SUB`. `AUTH_REFRESH` replaces the connection's identity; it must keep the same subject, otherwise the connection closes with `4002`. The server sends `AUTH_EXPIRING` before expiry, revalidates periodically, and closes with `4006` once the identity is no longer valid.
