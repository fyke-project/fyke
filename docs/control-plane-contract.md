# Control Plane Contract & Exporter Specification

This document defines the interface contract between the **Fyke Agent** (in-JVM library running in customer applications) and any compatible **Fyke Control Plane**.

Following **ADR D-001** (actions in-JVM), **ADR D-002** (air-gap-first), and **ADR D-009** (metadata-only SaaS boundary), the agent acts strictly as an egress-only client. It exports telemetry metadata and executes cryptographically signed remediation commands locally.

---

## 1. Guiding Principles

1. **Air-Gap Default:** The agent has zero network surfaces and makes zero remote network calls by default. Integration with a control plane requires explicitly setting `fyke.control-plane.enabled=true` and providing credentials.
2. **Egress-Only Architecture:** Corporate networks reject inbound ingress ports. The Fyke agent initiates a persistent outbound TLS connection to the control plane over gRPC (HTTP/2).
3. **Metadata-Only by Default:** The agent strips payload data client-side before sending telemetry. Telemetry contains business keys, event types, status transitions, sizes, and SHA-256 hashes — never raw payload bytes.
4. **Local Action Execution:** All state mutations (`replayOutbox`, `retryInbox`, `discardInbox`) execute inside the application JVM with its local credentials and database connections. Commands are relayed down the stream and verified before execution.
5. **Fail-Safe Isolation:** If the control plane or internet connection fails, local outbox and inbox reliability operates completely unaffected.

---

## 2. Protocol Buffers Specification (`fyke_controlplane.proto`)

```protobuf
syntax = "proto3";

package dev.fyke.controlplane.v1;

import "google/protobuf/timestamp.proto";

option java_multiple_files = true;
option java_package = "dev.fyke.controlplane.v1";
option java_outer_classname = "FykeControlPlaneProto";

// ============================================================================
// Service Definition
// ============================================================================

service FykeControlPlaneService {
  // Single, persistent, bi-directional stream initiated by the Fyke Agent.
  rpc ConnectStream(stream AgentEnvelope) returns (stream ControlPlaneEnvelope);
}

// ============================================================================
// Envelopes
// ============================================================================

message AgentEnvelope {
  string trace_id = 1;

  oneof payload {
    HandshakeRequest handshake = 2;
    HeartbeatReport heartbeat = 3;
    TelemetryBatchReport telemetry_batch = 4;
    CommandExecutionResult command_result = 5;
  }
}

message ControlPlaneEnvelope {
  string correlation_id = 1;

  oneof payload {
    HandshakeResponse handshake_ack = 2;
    HeartbeatAck heartbeat_ack = 3;
    CommandEnvelope command = 4;
  }
}

// ============================================================================
// Handshake & Capability Negotiation
// ============================================================================

message HandshakeRequest {
  AppIdentity identity = 1;
  repeated AgentCapability capabilities = 2;
  google.protobuf.Timestamp startup_time = 3;
}

message HandshakeResponse {
  bool accepted = 1;
  string session_id = 2;
  string rejection_reason = 3;

  int32 heartbeat_interval_seconds = 4;   // default: 15
  int32 telemetry_flush_interval_ms = 5;  // default: 1000
  int32 telemetry_max_batch_size = 6;     // default: 500
}

message AppIdentity {
  string tenant_id = 1;
  string app_name = 2;
  string environment = 3;
  string instance_id = 4;
  string agent_version = 5;
  string spring_boot_version = 6;
  string jvm_version = 7;
  repeated string active_binders = 8;
}

enum AgentCapability {
  AGENT_CAPABILITY_UNSPECIFIED = 0;
  AGENT_CAPABILITY_OUTBOX_CAPTURE = 1;
  AGENT_CAPABILITY_INBOX_CAPTURE = 2;
  AGENT_CAPABILITY_DLQ_CAPTURE = 3;
  AGENT_CAPABILITY_ACTIONS_IN_JVM = 4;
  AGENT_CAPABILITY_PAYLOAD_FETCH = 5; // strictly enabled in Self-Hosted EE mode only
}

// ============================================================================
// Heartbeat & Health
// ============================================================================

message HeartbeatReport {
  google.protobuf.Timestamp timestamp = 1;

  int64 fyke_outbox_pending_count = 2;
  int64 fyke_outbox_oldest_age_seconds = 3;

  int64 fyke_inbox_pending_count = 4;
  int64 fyke_inbox_oldest_age_seconds = 5;

  int64 fyke_dlq_unresolved_count = 6;

  repeated string active_partition_locks = 7;
  bool is_db_connected = 8;
  bool is_broker_connected = 9;
}

message HeartbeatAck {
  google.protobuf.Timestamp acknowledged_at = 1;
}

// ============================================================================
// Telemetry Batching (Metadata-Only)
// ============================================================================

message TelemetryBatchReport {
  repeated EventMetadataRecord records = 1;
}

enum EventChannel {
  EVENT_CHANNEL_UNSPECIFIED = 0;
  EVENT_CHANNEL_OUTBOX = 1;
  EVENT_CHANNEL_INBOX = 2;
  EVENT_CHANNEL_DLQ = 3;
}

enum EventStatus {
  EVENT_STATUS_UNSPECIFIED = 0;
  EVENT_STATUS_NEW = 1;
  EVENT_STATUS_DISPATCHING = 2;
  EVENT_STATUS_PUBLISHED = 3;
  EVENT_STATUS_DEAD = 4;
  EVENT_STATUS_CONSUMED = 5;
  EVENT_STATUS_REPLAYED = 6;
}

message EventMetadataRecord {
  string event_id = 1;
  EventChannel channel = 2;
  string event_type = 3;
  string destination = 4;
  string target = 5;
  string business_key = 6;
  string idempotency_key = 7;
  EventStatus status = 8;
  string content_type = 9;
  int64 payload_size_bytes = 10;
  string payload_sha256 = 11;
  map<string, string> sanitized_headers = 12;
  string correlation_id = 13;
  string trace_id = 14;
  int32 attempts = 15;
  string error_reason = 16;
  string consumer = 17;
  google.protobuf.Timestamp created_at = 18;
  google.protobuf.Timestamp updated_at = 19;
  google.protobuf.Timestamp completed_at = 20;
}

// ============================================================================
// Remote Commands
// ============================================================================

message CommandEnvelope {
  string command_id = 1;
  google.protobuf.Timestamp issued_at = 2;
  string issued_by_user = 3;
  string signature = 4; // Ed25519 signature of the command payload

  oneof command {
    ReplayOutboxCommand replay_outbox = 10;
    RetryOutboxCommand retry_outbox = 11;
    RetryInboxCommand retry_inbox = 12;
    DiscardInboxCommand discard_inbox = 13;
    FetchPayloadCommand fetch_payload = 14;
  }
}

message ReplayOutboxCommand {
  string outbox_id = 1;
  string business_key = 2;
}

message RetryOutboxCommand {
  string outbox_id = 1;
  string business_key = 2;
}

message RetryInboxCommand {
  string dlq_id = 1;
  string business_key = 2;
}

message DiscardInboxCommand {
  string dlq_id = 1;
  string reason = 2;
}

message FetchPayloadCommand {
  EventChannel channel = 1;
  string event_id = 2;
}

// ============================================================================
// Command Execution Result
// ============================================================================

enum CommandOutcome {
  COMMAND_OUTCOME_UNSPECIFIED = 0;
  COMMAND_OUTCOME_SUCCESS = 1;
  COMMAND_OUTCOME_FAILED = 2;
  COMMAND_OUTCOME_NOT_FOUND = 3;
  COMMAND_OUTCOME_UNAUTHORIZED = 4;
  COMMAND_OUTCOME_DISABLED = 5;
}

message CommandExecutionResult {
  string command_id = 1;
  CommandOutcome outcome = 2;
  google.protobuf.Timestamp executed_at = 3;
  string error_message = 4;

  bytes raw_payload = 5;
  string payload_content_type = 6;
}
```

---

## 3. Agent Lifecycle & Connection Management

```
┌──────────────┐                                          ┌───────────────┐
│  Fyke Agent  │                                          │ Control Plane │
└──────┬───────┘                                          └───────┬───────┘
       │                                                          │
       │ 1. gRPC HTTP/2 TLS Handshake (Authorization: Bearer key) │
       │─────────────────────────────────────────────────────────▶│
       │ 2. HandshakeRequest (identity, capabilities)             │
       │─────────────────────────────────────────────────────────▶│
       │ 3. HandshakeResponse (accepted, intervals)               │
       │◀─────────────────────────────────────────────────────────│
       │                                                          │
       │ ── Continuous Stream ─────────────────────────────────── │
       │ 4. HeartbeatReport (every 15s)                           │
       │─────────────────────────────────────────────────────────▶│
       │ 5. TelemetryBatchReport (every 1000ms, if events exist)  │
       │─────────────────────────────────────────────────────────▶│
       │ 6. CommandEnvelope (ReplayOutboxCommand)                 │
       │◀─────────────────────────────────────────────────────────│
       │ 7. CommandExecutionResult (SUCCESS)                      │
       │─────────────────────────────────────────────────────────▶│
```

### Reconnection & Backoff
If the gRPC connection drops:
1. The agent enters a reconnect loop with exponential backoff and jitter (initial: 1s, multiplier: 2.0, max: 60s).
2. The core poller and domain event publication continue running uninterrupted.
3. Telemetry records are buffered in an in-memory ring buffer.

### Bounded Ring Buffer & Backpressure
* **Capacity:** Bounded queue of 10,000 records (configurable via `fyke.control-plane.buffer-capacity`).
* **Overflow Policy:** If disconnected long enough for the buffer to fill, the oldest records are dropped (`DropOldest`), and an internal gauge `fyke_controlplane_dropped_records_total` is incremented.
* **Under no circumstances will the agent block domain transactions or outbox polling due to control plane backpressure.**

---

## 4. Configuration Reference

```yaml
fyke:
  control-plane:
    enabled: false                         # Default: false (air-gapped)
    endpoint: "connect.fyke.dev:443"       # Control plane gRPC endpoint
    api-key: "${FYKE_CONTROL_PLANE_KEY}"   # Authentication bearer key
    environment: "production"              # Application environment tag
    app-name: "${spring.application.name}" # Application name
    tls:
      enabled: true                        # TLS transport
    command-verification:
      enabled: true                        # Enforce Ed25519 signature checks
      public-key: "base64-pubkey-here"     # Control plane public key
    buffer:
      capacity: 10000                      # In-memory ring buffer size
      flush-interval-ms: 1000              # Flush interval for telemetry batches
      batch-size: 500                      # Max records per batch
    allow-payload-fetch: false             # Set true ONLY in in-VPC Self-Hosted EE
```
