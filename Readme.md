# Async Task Queue with Retries and Compensation (Simplified)

Caleb McCarthy

## Context (what this is):

A small job queue that accepts jobs (e.g., “send email”, “generate report”), runs them concurrently with retries, and **executes a compensation action** to undo side effects if a job ultimately fails.

## Build (must-haves)

* **Submission:** POST /v1/jobs { type, payload, idempotencyKey? } → { jobId }.
* **Status:** GET /v1/jobs/{jobId} → { status, attempts, lastError?, startedAt?, completedAt? } where status ∈ QUEUED|RUNNING|SUCCEEDED|FAILED|COMPENSATED.
* **Workers & queue:** fixed-size worker pool; bounded queue; when full, return 429 with a clear message.
* **Retries:** exponential backoff with jitter; max attempts; on max failure → FAILED.
* **Compensation:** for each type, expose execute(payload) and compensate(lastKnownState); after final failure, run compensation and mark COMPENSATED (record if compensation fails).
* **Idempotency:** same idempotencyKey returns the first job’s result; handlers avoid duplicate effects.
* Tests proving retry → failure → compensation and demonstrating two job types (one side-effectful).

## Acceptance checks

* Backpressure works (bounded queue → 429).
* Retries include jitter; after max attempts, compensation runs.
* Duplicate submissions (same key) don’t duplicate effects.
* Logs/status clearly show execution → failure → compensation.



# Solution how to check acceptance checks

Run test with,

```
cargo test -- --nocapture test_retry_then_compensation

cargo test -- --nocapture  test_idempotency_key

cargo test -- --nocapture  test_backpressure_returns_429

```