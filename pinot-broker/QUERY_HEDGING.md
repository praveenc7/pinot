<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Single-stage request hedging

Broker request hedging is a disabled-by-default recovery mechanism for short server stalls. After a configurable
delay, a broker can duplicate one still-outstanding server request to an exact replica. The first usable response is
reduced; the other response is ignored for reduction but retained for adaptive-routing accounting until it arrives or
the original query deadline expires.

Hedging does not change primary replica selection, extend the query timeout, reserve reduction time, cancel the losing
request, or require a server or wire-protocol change.

An explicit query cancellation suppresses a pending hedge decision and targets both primary and potential alternate
servers so an already-dispatched duplicate receives the same cancellation.

## Eligibility

Hedging applies only to single-stage Netty queries for physical OFFLINE, REALTIME, or hybrid tables using
`replicaGroup` or `strictReplicaGroup` routing. It is not used for EXPLAIN queries, secondary workloads, logical
tables, gRPC transport, or multi-stage queries.

An alternate is eligible only when one server can process every required segment assigned to the primary. Optional
segments are included only when online on that alternate. A server already selected as any OFFLINE or REALTIME primary
for the query cannot be used as its hedge.

## Configuration

| Key | Default | Meaning |
| --- | ---: | --- |
| `pinot.broker.query.router.hedging.enabled` | `false` | Enables broker request hedging. |
| `pinot.broker.query.router.hedging.delay.ratio` | `0.5` | Fraction of the post-dispatch remaining timeout to wait. |
| `pinot.broker.query.router.hedging.delay.min.ms` | `25` | Minimum hedge delay. |
| `pinot.broker.query.router.hedging.delay.max.ms` | `500` | Maximum hedge delay. |
| `pinot.broker.query.router.hedging.max.hedges.per.query` | `1` | V1 requires exactly one. |
| `pinot.broker.query.router.hedging.max.extra.request.ratio` | `0.01` | Maximum admitted hedge-to-primary-send ratio. |
| `pinot.broker.query.router.hedging.budget.window.ms` | `60000` | Rolling budget window. |
| `pinot.broker.query.router.hedging.max.concurrent.requests` | `32` | Maximum active hedge requests per broker. |

The delay is calculated after primary dispatch:

```text
clamp(round(remainingTimeoutMs * delayRatio), minDelayMs, maxDelayMs)
```

No hedge is scheduled when that delay reaches or exceeds the remaining query time.

The rolling ratio has no startup or stored credit:

```text
hedgesInWindow + 1 <= floor(primarySendsInWindow * maxExtraRequestRatio)
```

At the default one-percent ratio, fewer than 100 successful primary sends in the current window permit no hedge.
Admissions remain charged when hedge transmission fails, preventing repeated failures from bypassing the load limit.

## Response behavior

An exception-free DataTable or one containing only client errors can win. A server-error DataTable waits only when its
duplicate is already in flight. If both attempts fail, the primary error is retained when available. If neither server
returns a DataTable, the existing server-not-responding behavior is preserved.

Public `numServersQueried` and `numServersResponded` remain logical primary counts. Physical hedge activity is exposed
through hedge metrics and the optional `hedgeStats` query-log field. An explicit channel failure still marks the
affected physical server unhealthy even when the duplicate rescues the logical request.

The duplicate never runs past the deadline the broker itself honors. Because it is dispatched after a delay, its
`timeoutMs` query option is rewritten at dispatch time to the time remaining before the original deadline, so the
alternate stops working when the broker stops waiting. The query's overall deadline is unchanged.

A hedge that fails to transmit is not reported to the broker failure detector. Alternates carry ordinary primary
traffic for other queries and are evaluated there, so a host is never taken out of rotation on hedge-only evidence.

## Rollout and rollback

Deploy with hedging disabled first. For a broker canary, enable the flag on a small subset and monitor:

- `HEDGE_REQUESTS_SENT`, `HEDGE_WINS`, `PRIMARY_WINS_AFTER_HEDGE`, and `HEDGE_ALL_ATTEMPTS_FAILED`;
- ratio, concurrency, deadline, no-alternate, and no-outstanding skip counters;
- `ACTIVE_HEDGE_REQUESTS`, `HEDGE_PRIMARY_REQUESTS_LAST_WINDOW`, and `HEDGE_REQUESTS_LAST_WINDOW`;
- hedge response latency, server-not-responding errors, reduction timeouts, broker latency/CPU, server request rate/CPU,
  and routing-stats queue size.

Confirm the rolling extra-request ratio remains within the configured bound and public queried-server counts do not
increase. Hedging can reduce missing-server failures but cannot guarantee elimination of reduction timeouts when a
response arrives near the unchanged original deadline.

Rollback requires only setting `pinot.broker.query.router.hedging.enabled=false`. There is no stored state, segment
format, server-version, or protocol dependency.
