# taskflow

A general-purpose distributed job queue and task runner. Clients submit jobs via HTTP API; workers process them asynchronously. The system tracks job state, handles failures, and provides status visibility.

## Problem Statement

Applications often need to run work that is too slow, too resource-intensive, or too unreliable to execute synchronously in an HTTP request. Examples: sending emails, resizing images, calling slow third-party APIs, generating reports.

This system decouples the **submission** of work from the **execution** of work. A client submits a job and gets an ID back immediately. A worker picks it up and processes it in the background. The client can poll for status.

---

## Functional Requirements

- `POST /jobs` — submit a job, returns a job ID immediately
- `GET /jobs/{id}` — returns current status and result of a job
- Jobs are processed asynchronously by one or more workers
- Each job has a type (e.g. `SEND_EMAIL`, `RESIZE_IMAGE`) and a JSON payload
- Failed jobs must be recorded with an error message

## Non-Functional Requirements

- The API must remain responsive under load, job submission must never block on processing
- Job state must be durable, so a server restart must not lose jobs
- Workers must be able to run concurrently without processing the same job twice

**What do we mean by functional and non-functional requirements?**
Functional requirements describe the features and behaviour of a system (GET, POST), while non-functional requirements describe how well the system performs the desired task (PERFORMANCE, AVAILABILITY, ETC...)

## Future Addition

- Job cancellation
- Priority queues
- Authentication / API keys
- Admin dashboard / UI

---

## Job Lifecycle

This is the core of the system.

```
             ┌─────────┐
             │ PENDING │  ← job submitted, sitting in queue
             └────┬────┘
                  │  worker picks it up
                  ▼
             ┌─────────┐
             │ RUNNING │  ← worker is actively processing
             └────┬────┘
           ┌──────┴──────┐
           ▼             ▼
      ┌──────────┐  ┌──────────┐
      │ SUCCEEDED│  │  FAILED  │  ← terminal states
      └──────────┘  └──────────┘
```

**Key constraint:** the transition from PENDING → RUNNING must be atomic. Two workers must never both claim the same job.

---

## Architecture

```
Client
  │
  ▼
Spring Boot API
  ├── POST /jobs     →  persist job (PENDING)  →  publish to Kafka topic
  └── GET /jobs/{id} →  read job state from Postgres

Kafka Topic: jobs.submitted
  │
  ▼
Worker (Kafka Consumer)
  ├── poll for messages
  ├── mark job RUNNING in Postgres
  ├── execute job handler (by job type)
  └── mark job SUCCEEDED or FAILED in Postgres
```

**Why Kafka and not just a DB queue?**
You could poll Postgres directly. Kafka gives you replay, consumer groups, horizontal scaling of workers, and decoupling of producer from consumer. Kafka is not explicitely needed here, these things can be done by polling a database and work fine functionally, the reason kafka is used here is to show how the industry solves these problems at scale.

**Why persist to Postgres at all if Kafka holds the messages?**
Kafka is not a database. It does not give you queryable job state. Postgres is the source of truth for job status.

---

## Data Model

### jobs table

| Column      | Type      | Notes                                    |
| ----------- | --------- | ---------------------------------------- |
| id          | UUID      | primary key, generated on submission     |
| type        | VARCHAR   | e.g. SEND_EMAIL, RESIZE_IMAGE            |
| payload     | JSONB     | arbitrary input data for the job         |
| status      | VARCHAR   | PENDING, RUNNING, SUCCEEDED, FAILED      |
| result      | JSONB     | output data on success, null otherwise   |
| error       | TEXT      | error message on failure, null otherwise |
| created_at  | TIMESTAMP |                                          |
| updated_at  | TIMESTAMP |                                          |
| started_at  | TIMESTAMP | when worker claimed the job              |
| finished_at | TIMESTAMP | when job reached terminal state          |

---

## API

### POST /jobs

Submit a new job.

**Request**

```json
{
  "type": "SEND_EMAIL",
  "payload": {
    "to": "user@example.com",
    "subject": "Welcome",
    "body": "Hello!"
  }
}
```

**Response — 202 Accepted**

```json
{
  "jobId": "a1b2c3d4-...",
  "status": "PENDING"
}
```

---

### GET /jobs/{id}

Get current state of a job.

**Response — 200 OK (in progress)**

```json
{
  "jobId": "a1b2c3d4-...",
  "type": "SEND_EMAIL",
  "status": "RUNNING",
  "createdAt": "2026-06-05T10:00:00Z",
  "startedAt": "2026-06-05T10:00:01Z"
}
```

**Response — 200 OK (completed)**

```json
{
  "jobId": "a1b2c3d4-...",
  "type": "SEND_EMAIL",
  "status": "SUCCEEDED",
  "result": { "messageId": "msg_xyz" },
  "createdAt": "2026-06-05T10:00:00Z",
  "startedAt": "2026-06-05T10:00:01Z",
  "finishedAt": "2026-06-05T10:00:02Z"
}
```

**Response — 200 OK (failed)**

```json
{
  "jobId": "a1b2c3d4-...",
  "type": "SEND_EMAIL",
  "status": "FAILED",
  "error": "SMTP connection refused",
  "createdAt": "2026-06-05T10:00:00Z",
  "startedAt": "2026-06-05T10:00:01Z",
  "finishedAt": "2026-06-05T10:00:03Z"
}
```

---

## Tech Stack

| Layer            | Choice                   | Notes                                |
| ---------------- | ------------------------ | ------------------------------------ |
| Language         | Java 21                  |                                      |
| Framework        | Spring Boot 3            |                                      |
| Database         | PostgreSQL               | durable job state                    |
| Message broker   | Kafka                    | job transport                        |
| Containerization | Docker Compose           | Postgres + Kafka + Zookeeper locally |
| Testing          | JUnit 5 + Testcontainers | spin up real Kafka/Postgres in tests |

---

## Getting Started

> Prerequisites: Java 21, Docker, Docker Compose

```bash
git clone https://github.com/Jatti045/taskflow.git
cd taskflow
docker compose up -d
./mvnw spring-boot:run
```

Service runs on `http://localhost:8080`.

---

## Project Structure

```
src/
├── main/
│   ├── java/com/taskflow/
│   │   ├── config/            # Kafka config, App config
│   │   ├── controller/        # HTTP layer — JobController
│   │   ├── dto/               # Data Transfer Objects
│   │   ├── handler/           # One class per job type
│   │   ├── model/             # Job entity, JobStatus enum
│   │   ├── repository/        # JobRepository (Spring Data JPA)
│   │   ├── service/           # JobService — submission logic
│   │   ├── worker/            # Kafka consumer + job dispatch
│   └── resources/
│       ├── application.properties
└── test/
    ├── java/com/taskflow/
    │   ├── services/

```

---

## Decision Log

**What happens when a worker crashes while processing a job — does the job get retried or stay stuck in RUNNING?**
The job is re-queued in Kafka to be picked up by the next available worker via startup recovery.

**How many retry attempts should a failed job get before being marked permanently failed?**
The retry count is dynamic, set to the number of attempts that achieves the job's SLO plus a safety margin. The retry count is determined by measuring what percentage of submission calls fail on the first attempt, what percentage of those failures succeed on each subsequent retry, and what the acceptable failure rate is for that job type — for critical jobs (e.g. transactional email) a 99.9%+ success rate is required, so if 3 retries only yields 99.8% then a 4th retry is needed, whereas for non-critical jobs 99% may be acceptable with just 1–2 retries. In other words, the retry count is the number of attempts needed to meet the job's Service Level Objective (SLO) plus a safety margin.

**Should job payloads be validated at submission time or only at processing time?**
At submission time, avoids wasting worker resources on invalid jobs.

**Should workers be threads within the same process, or separate deployable services?**
Threads within the same process, simpler to deploy and sufficient for this scale.

**What is the maximum payload size for a job?**
No hard limit, payload size varies by job type.

**Should `GET /jobs/{id}` return the full result payload or just status + a reference?**
Status + a reference — the full payload is available separately if needed.

**Why use virtual threads for the Kafka consumer worker pool instead of a fixed platform thread pool?**
Taskflow workers are almost entirely I/O bound, they read/write from postgres and consume kafka messages. When a virtual thread blocks on any of these operations, the JVM parks the virtual thread and unmounts it from its carrier thread. That carrier thread is free to then run other virtual threads. With platform threads, the OS thread sits idle for the entire duration of the I/O wait. Virtual threads eliminate that idle time, allowing the carrier threads to keep jobs running.

---

## Known Limitations

- No visibility into queue depth
- Workers are threads in the same process, not separate deployables
- No authentication on API endpoints
- scheduled jobs are lost on application restart because ThreadPoolTaskScheduler is in-memory.

---

## Roadmap

- [x] v1: submit, process, status — Postgres + Kafka, Docker Compose
- [x] v2: automatic retry with backoff on failure
- [x] v3: job scheduling — submit a job to run at a future time
- [x] v4: dead letter queue for permanently failed jobs
- [ ] v5: metrics endpoint — queue depth, throughput, failure rate
