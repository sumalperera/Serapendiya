# ADR-023 proposal: workload identity, authorization and publish outcomes

Status: **PROPOSED — future design only.** No identity, authorization,
idempotency, tenancy, audit or MCP code is authorized by this document. Exact
security policies, persistent formats and gate scope require separate review
and approval. The local blueprint's current-state table remains authoritative.

## Threat model and boundary

Agent hosts may be misled by untrusted events, tool output or metadata. A payload,
schema, header or model-produced argument does not grant authority. The server
authenticates and authorizes; least privilege limits harm but does not prevent
exfiltration using permissions the principal already has. Sensitivity labels,
information-flow rules and approval policies are candidates for higher-risk
workflows, not claimed protections.

## Principals and namespace

Evaluate distinct workload principals, expiring credentials, resource/action
permissions, bounded discovery and optional later delegation. Map authenticated
Kafka SASL or mTLS principals and native API principals to the same authorization
decision model. MCP must not be an administrative shortcut. Separately authorize
discovery, reads, writes, subscriptions, status queries and administration;
bound byte use and request counts.

Candidate: a stable opaque tenant identifier and topic UUID form the log identity;
mutable authorization maps principals to tenants and resource/action grants;
topic names are tenant-local presentation. A fixed, explicitly defined default
tenant could let single-tenant deployments retain their identity when tenancy is
introduced. The durable identity source, identifier allocation and collision
handling, default-tenant migration, metadata restore, topic deletion/recreation,
replication and recovery rules require design and tests. A tenant identifier in
a path alone is not authorization. No identifier or path layout is implemented
or reserved by this proposal.

Kafka clients do not have a general tenant field in the supported Kafka request
model; evaluate resolving a tenant from the authenticated listener principal or
an explicitly negotiated context. Multi-tenant principals and delegation need
unambiguous selection and authorization. Never infer a trusted tenant from an
untrusted topic-name prefix, client ID or producer header. TLS and multi-tenant
isolation remain networked-production gates, not claims about today's local log.

## Outcomes and idempotency

A future native publish API should distinguish confirmed committed, definitively
rejected and outcome unknown. Timeout or an error does not prove non-commit.
Confirmed results must name the actual local or replicated durability boundary;
Kafka protocol responses and `acks=0` retain their own semantics. Authorized
status queries may themselves be unavailable, pending, expired or unknown.

Compare storing authenticated-producer identity and idempotency key in a batch
envelope with transactional state logs and other approaches. An envelope alone
does not guarantee atomic deduplication. Specify key scope across tenants,
principals, streams and partitions; concurrent duplicates; same key with
different payload; canonical payload hashing; retention, compaction, truncation,
migration, restart, capacity and resolution of uncertain outcomes. No exactly-once
or indefinite deduplication claim without tests. A failed checkpoint directory
sync can have an unknown outcome, and status lookup may be unavailable in the
terminal storage state until recovery, which cannot resolve every possible failure.

One candidate scopes keys by `(tenant, authenticated principal, partition, key)`
and uses the single partition owner to serialize check-and-append. That scope
must be exposed to callers: retries routed to a different partition are not
deduplicated without additional coordination. If keys are batch-level, specify
the identity and outcome of every record in a batch and any delegation/proxy
behavior. Bound per-principal memory and persisted state; define canonical content
digest encoding and collision assumptions and reject a known key with changed
content. Compare digest algorithms under the threat model and measure costs.

An index snapshot is merely a cache **only if all required deduplication evidence
remains reconstructible from retained, validated logs**. If retention or
compaction removes that evidence, a durable snapshot or equivalent state becomes
authoritative and needs its own atomicity, replication and recovery design.
After forgetting a key, the system cannot distinguish an expired key from one
never seen without retained tombstones, signed time-bound keys or equivalent
evidence. Do not promise a reliable `expired` response or safely treat a retry
as new until that mechanism and its bounded retention contract are specified.
Kafka producer PID/epoch/sequence deduplication is a separate protocol and must
not be conflated with native request keys.

## Provenance and interoperability

Compare broker-owned envelope metadata and a protected audit stream. Bind
provenance to authenticated principals, protect its integrity and privacy, and
define retention, compaction and replication. Kafka clients do not automatically
receive internal envelope fields. Exposing provenance requires an explicit
interface or separately specified Kafka-compatible mechanism.

Proposed default: all Kafka headers are producer-controlled data regardless of
their names; do not silently reject, strip or treat any such header as attested.
Expose server-attested provenance through an explicitly defined native field,
separate from producer headers. Kafka clients cannot automatically see internal
envelope provenance and must not infer it from a header name. An opt-in header
policy would need a compatibility decision and documented error semantics.

An interned principal ID per batch could reduce space, but requires a durable,
versioned identity mapping and ordering that prevents a committed batch from
referencing an unavailable mapping. Identity reuse, restore, retention,
delegation, privacy and replicated recovery all need design. A batch arriving on
one authenticated connection does not automatically establish the original
end-user identity behind a proxy or delegation chain.
No identity or idempotency fields are reserved in the experimental container;
the extension mechanism and migration require ADR-003 review first.

## Delegation, revocation and audit

Prefer established credential mechanisms and providers with issuer, audience,
expiry and scope validation; do not build a custom identity provider by default.
Decide cache lifetime, policy-change propagation and revocation-service failure.
Short expiry alone is not immediate revocation; long-running fetches and existing
connections need explicit reauthorization or termination behavior.

Queue leases may bind to the claiming principal, but delegation, legitimate
worker rotation, ownership transfer and stale-token fencing require a separate
queue design. Avoid resource-existence leaks where practical while preserving
required protocol error semantics.

Audit authentication, denials and administrative events without credentials
or payloads by default. The destination, access, sampling and failure policy
need a threat and availability review. Neither silently dropping data-path audit
records nor blocking every operation during an audit outage is universally safe.

## Sequencing and acceptance

- G1: complete storage/recovery; use this proposal to identify design constraints.
  Do not create agent-security scaffolding.
- G2: review Kafka principal mapping and access-control boundaries during a
  separately authorized compatibility gate. Any insecure local test mode must
  be explicit. Do not expose a production service before required security gates.
- G4/G5: implement scoped security and hardened transport only after explicit
  authorization and accepted security/storage ADRs.
- R2 or later: MCP starts scoped and read-only. Mutating tools require separate
  authorization, outcome semantics and adversarial tests. MCP credentials must
  be audience-bound and not blindly forwarded to unrelated backends.

When authorized, test cross-tenant isolation, Kafka-listener bypass, forged
provenance, header compatibility, revocation during active connections,
identity-bound stale ACKs, concurrent retries, crashes around deduplication,
expired status, oversized requests, quotas, discovery leaks and malicious event
instructions. No current capability or compatibility is claimed here.
