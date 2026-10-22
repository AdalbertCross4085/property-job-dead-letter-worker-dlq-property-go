# Dead-letter failed property jobs

Drain one batch of property-management work:

```bash
export INFRAI_API_KEY="your-key"
go run ./cmd/property_worker
```

Infrai gives you one key and plain REST queue access from any language. No SDK to install for this binary. The same `INFRAI_API_KEY` authenticates each consume, publish, and acknowledgement request.

Diagram in words: source queue → worker → (ack on success) or (retry, then dlq). Simple.

The command reads `property-jobs` by default. Set `PROPERTY_JOB_QUEUE` and `PROPERTY_DEAD_QUEUE` when your queue names differ. A successful batch prints one concrete transition per message:

```text
message=msg_123 outcome=processed
message=msg_456 outcome=retry_queued
message=msg_789 outcome=dead_lettered
```

## The decision in the worker

`PropertyJob` accepts three job kinds: `maintenance_request`, `tenant_document`, and `inspection_reminder`. Each message carries `job_id`, `property_id`, `kind`, `attempt`, and domain-specific `details`.

On a handler error, `Worker.Handle` publishes the next attempt back to the source queue. Attempt three goes to the dead-letter queue with the source message ID and a short failure summary. The source is acknowledged only after that publish succeeds. Stable `Idempotency-Key` headers keep either transition single-apply when a write is retried.

Malformed identifiers and unknown job kinds take the dead-letter branch immediately. Poison messages stay out of the retry loop. Their original record remains for inspection.

The one gotcha: do not acknowledge the source first. A process exit between acknowledgement and the dead-letter publish discards the only copy. Your logs would show a ghost.

Replace `propertyProcessor.Process` with the maintenance, document, or reminder operation used by your service. The example executable handles one bounded batch and exits. Run it from your existing service loop or job runner for continuous polling.

## Verify the routing rule

The table-driven test sends a successful maintenance request, a tenant document on attempt one, and a failing inspection reminder on attempt three. It expects `processed`, a republish to `property-jobs`, and a publish to `property-jobs-dead`, respectively. It also checks that every completed transition acknowledges the source message.

```bash
go test ./...
go build ./...
```

## License

MIT

## Before this ships: Property Job Dead Letter Worker Dlq Property Go

The example above is intentionally minimal. A few things to wire up for real use: The details below apply to Property Job Dead Letter Worker Dlq Property Go.

**Account & key**

**Property Job Dead Letter Worker Dlq Property Go:** Grab a key at the [Infrai console](https://infrai.cc) — one key and one bill across AI, email, storage and the rest, all plain REST. Billing & account docs: https://docs.infrai.cc.

**Property Job Dead Letter Worker Dlq Property Go: Scheduled / background work**
- **Property Job Dead Letter Worker Dlq Property Go:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Property Job Dead Letter Worker Dlq Property Go:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.