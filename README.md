# Distributed Job Scheduling & Execution Platform

A Spring Boot based distributed job scheduler using PostgreSQL, Redis and Apache Kafka.

## Architecture

```text
Client
  |
  v
REST API (Spring Boot)
  |
  +--> PostgreSQL  ---- persistent job record
  |
  +--> Redis       ---- fast current-status lookup
  |
  v
Scheduler
  |
  v
Kafka topic: job-execution
  |
  v
Worker(s)
  |
  +--> PostgreSQL
  |
  +--> Redis
```

## Job lifecycle

```text
SCHEDULED
    |
    | scheduledAt reached
    v
QUEUED
    |
    | Kafka worker claims job
    v
RUNNING
   / \
  /   \
 v     v
SUCCESS FAILED
          |
          | retries available
          v
      SCHEDULED
          |
          v
        QUEUED
```

A job can also transition from `SCHEDULED` or `QUEUED` to `CANCELLED`.

## Technology

- Java 21+ / Spring Boot
- Spring Web
- Spring Data JPA / Hibernate
- PostgreSQL 16
- Redis 7
- Apache Kafka 4
- Docker Compose
- Maven

## Start infrastructure

From `job-scheduler/`:

```powershell
docker compose up -d
```

Verify:

```powershell
docker ps
```

You should see:

- `job-scheduler-postgres`
- `job-scheduler-redis`
- `job-scheduler-kafka`

## Start application

On Windows:

```powershell
.\mvnw.cmd clean spring-boot:run
```

The application runs on:

```text
http://localhost:8080
```

The project is configured for `Asia/Kolkata`. Keep the JVM timezone setting already used by the working local environment:

```powershell
$env:JAVA_TOOL_OPTIONS="-Duser.timezone=Asia/Kolkata"
```

## APIs

### Create a job

```http
POST /api/jobs
Content-Type: application/json
```

Example:

```json
{
  "name": "Successful Job",
  "type": "TEST",
  "payload": "Hello",
  "scheduledAt": "2026-10-05T18:30:00",
  "maxRetries": 3
}
```

The job is initially stored as `SCHEDULED`.

The scheduler detects it when `scheduledAt` is reached, changes it to `QUEUED`, and publishes the ID to Kafka.

### Get all jobs

```http
GET /api/jobs
```

### Get one job

```http
GET /api/jobs/{id}
```

### Get current Redis status

```http
GET /api/jobs/{id}/status
```

Example response:

```json
{
  "jobId": 12,
  "status": "SUCCESS"
}
```

### Get job statistics

```http
GET /api/jobs/stats
```

### Cancel a job

```http
POST /api/jobs/{id}/cancel
```

Only `SCHEDULED` and `QUEUED` jobs can be cancelled.

A job already running cannot safely be cancelled by this worker implementation.

### Delete a job

```http
DELETE /api/jobs/{id}
```

## Testing success

Use a scheduled time a few seconds in the future:

```json
{
  "name": "Success Test",
  "type": "TEST",
  "payload": "Hello",
  "scheduledAt": "2026-10-05T18:30:00",
  "maxRetries": 3
}
```

Watch the Spring Boot terminal.

Expected flow:

```text
Job queued for execution: 13
Worker received job: 13
Job is now RUNNING: 13
Executing job: 13
Job completed successfully: 13
```

Then:

```http
GET /api/jobs/13/status
```

should return:

```json
{
  "jobId": 13,
  "status": "SUCCESS"
}
```

## Testing retries

Put `FAIL` anywhere in the payload:

```json
{
  "name": "Failure Test",
  "type": "TEST",
  "payload": "FAIL",
  "scheduledAt": "2026-10-05T18:30:00",
  "maxRetries": 3
}
```

The worker intentionally throws a simulated failure.

The job will:

```text
RUNNING
   |
   v
SCHEDULED
   |
   | 5 second retry delay
   v
QUEUED
   |
   v
RUNNING
```

This repeats until the retry limit is reached.

With `maxRetries = 3`, the initial attempt plus three retries can execute, after which the final state is:

```text
FAILED
```

The `errorMessage` is persisted in PostgreSQL.

## Testing cancellation

Create a job several minutes in the future:

```json
{
  "name": "Cancellation Test",
  "type": "TEST",
  "payload": "Hello",
  "scheduledAt": "2026-10-05T20:00:00",
  "maxRetries": 3
}
```

Then:

```http
POST /api/jobs/{id}/cancel
```

Expected:

```json
{
  "jobId": 14,
  "status": "CANCELLED"
}
```

The scheduler will not enqueue it, and a previously queued Kafka message will not execute it because the worker only claims `QUEUED` jobs.

## Distributed worker safety

The worker does not blindly execute every Kafka message.

It atomically changes:

```text
QUEUED -> RUNNING
```

in PostgreSQL.

If two workers receive duplicate messages for the same job, only one can successfully claim it. The other sees zero updated rows and skips execution.

This provides a simple database-backed execution claim without holding a database transaction open during the simulated job execution.

## Redis

Redis keys use:

```text
job:{id}:status
```

Example:

```text
job:12:status
```

Values include:

```text
SCHEDULED
QUEUED
RUNNING
SUCCESS
FAILED
CANCELLED
```

Keys expire after 24 hours. If a status key is missing, the API rebuilds it from PostgreSQL.

## Kafka

Kafka topic:

```text
job-execution
```

Messages contain only the job ID:

```text
12
```

The producer uses:

```text
job-12
```

as the Kafka message key.

Consumer group:

```text
job-worker-group
```

Multiple application instances can therefore participate as workers, while the PostgreSQL claim prevents duplicate execution of the same job.

## Important design limitation

This project uses PostgreSQL state transitions and Kafka separately. There is no Kafka/PostgreSQL transactional outbox.

Therefore, a production-grade version should add an Outbox Pattern so that changing a job to `QUEUED` and publishing its Kafka event can be made reliably recoverable if the application crashes between those two operations.

For a portfolio/distributed-systems project, the current implementation demonstrates:

- REST APIs
- persistence
- scheduling
- asynchronous messaging
- distributed worker execution
- Redis state caching
- retries
- failure handling
- cancellation
- worker identification
- duplicate execution protection
