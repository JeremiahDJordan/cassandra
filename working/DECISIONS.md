# CASSANDRA-21171 — Netty Option B prototype — DECISIONS log

Branch: `bufferpool-netty-transport` (off the JDK 25 enablement branch, which ships the marker/att
workaround as a safety net). Author: Jeremiah Jordan.

Goal: point the Netty TLS pipelines at Netty's own `PooledByteBufAllocator` so AES-GCM operands carry a
legal `att` chain on every JDK, removing the need for the marker/map/gate — at least for transport.

---

## Phase 0 — Architecture audit (before touching code)

### The JDK 25 GCM trigger, confirmed from source
`java.base/com/sun/crypto/provider/GaloisCounterMode.overlapDetection(ByteBuffer src, ByteBuffer dst)`
(JDK 25.0.3, src.zip lines 912-955):

- Only runs the att-walk when **BOTH `src.isDirect()` and `dst.isDirect()`**.
- Walks **both** att chains: `while (dsrc.attachment() != null) { srcaddr = getBufferAddress((Buffer) dsrc.attachment()); dsrc = (DirectBuffer) dsrc.attachment(); }` and the same for `ddst`.
- The cast `(Buffer) ...attachment()` throws `ClassCastException` if **either** operand's att is a
  Cassandra `Chunk` (not a `java.nio.Buffer`). So a single BufferPool buffer on *either* the wrap/unwrap
  source or destination is enough to trigger the CCE.

Implication: a complete source-level fix must ensure **both** GCM operands (src AND dst) of every
`SSLEngine.wrap`/`unwrap` are non-BufferPool (or att-legal) buffers.

### Where the SslHandler gets its operands (Netty 4.1.130.Final, verified via javap)
`io.netty.handler.ssl.SslHandler`:
- `allocate(ctx, n)` and `allocateOutNetBuf(ctx, ...)` both call **`ctx.alloc()`** = the channel
  allocator (`ChannelOption.ALLOCATOR`). These produce the unwrap-dest and wrap-dest (out-net) buffers.
- The allocator passed to `sslContext.newHandler(alloc, ...)` is **NOT** used for per-op wrap/unwrap
  buffers — so `SocketFactory.newSslHandler` passing a different allocator would NOT fix the operands.
  The lever is `ChannelOption.ALLOCATOR`, exactly as the design note's Option B says.
- wrap **source** = the application `ByteBuf` flowing through the pipeline (the frame payload).
- unwrap **source** = the inbound network `ByteBuf` (socket read buffer, from the channel allocator).

### The catch the design note understates: the frame payload is a BufferPool buffer independent of the channel allocator
- Internode + native-V5 outbound frames are built by `FrameEncoder.Payload`, whose `buffer` comes
  **directly** from `BufferPools.forNetworking().getAtLeast(..., OFF_HEAP)` (FrameEncoder.java:65), and
  `FrameEncoderCrc.encode` returns `GlobalBufferPoolAllocator.wrap(frame)` (FrameEncoderCrc.java:90) — a
  BufferPool-backed `ByteBuf`. This ByteBuf is the **wrap source** handed to SslHandler.
- Therefore, **swapping only `ChannelOption.ALLOCATOR` fixes the wrap/unwrap DESTINATION operands but
  NOT the wrap SOURCE** (the frame payload), which is still a BufferPool buffer with `att=Chunk`.
- The unwrap source (inbound net buffer) DOES come from the channel allocator, so swapping the allocator
  fixes the decrypt path's source.

Net: to remove BufferPool buffers from GCM operands entirely we must address (a) channel allocator
(unwrap src + both dests) AND (b) the frame payload buffer (wrap src). Pure "swap ALLOCATOR" alone is
necessary but not sufficient on the encrypt path.

### Native transport already mostly uses Netty's pool
- `CBUtil.allocator` = `new PooledByteBufAllocator(true)` (CBUtil.java:67), and the initial native
  pipeline sets `childOption(ALLOCATOR, CBUtil.allocator)` (PipelineConfigurator.java:155).
- The regression point for native is `configureModernPipeline` (PipelineConfigurator.java:299-300) which
  **overrides** the channel allocator back to `GlobalBufferPoolAllocator.instance` for V5+.

### GlobalBufferPoolAllocator consumers (safety-audit scope)
src/java consumers: AsyncStreamingOutputPlus, FrameEncoder{Crc,LZ4,Unprotected}, HandshakeProtocol,
InboundConnectionInitiator, OutboundConnectionInitiator, LocalBufferPoolAllocator, NettyStreamingChannel,
PipelineConfigurator, SimpleClient (client tool). `Wrapped.adopt()` extracts the bare ByteBuffer to hand
ownership to the inbound message-assembly path (FrameDecoder). This is the ownership coupling that makes a
blind channel-allocator swap on internode risky.

## Phase 1 — Empirical grounding of the bug (corrected model)

Added a test-only gate override in `BufferPool`: `cassandra.test.bufferpool.gcm_marker` (true/false) overrides the
JDK-feature auto-detect for `GCM_NEEDS_BUFFER_ATTACHMENT`. Verified via reflection: `false` => marker disabled,
default => enabled on JDK 25. This is the "neutralise the marker" hook the task requires.

Reproduced the bug end-to-end on JDK 25 with the marker disabled, using the canonical test:
```
ant testsome -Dtest.name=org.apache.cassandra.utils.memory.BufferPoolTest \
  -Dtest.methods=testAesGcmRoundTripWithPooledDirectBuffers \
  -Dtest.jvm.args="-Dcassandra.test.bufferpool.gcm_marker=false"
=> ClassCastException: Ref$DirectBufferRef cannot be cast to java.nio.Buffer   (the JDK 25 bug)
```
(The att is `DirectBufferRef` not `Chunk` because the unit-test JVM runs with `-Dcassandra.debugrefcount=true`
=> `REF_TRACE_ENABLED`; both are non-`java.nio.Buffer` atts, same class of failure. Property MUST be passed via
`-Dtest.jvm.args=...`; the ant junit task forwards `${test.jvm.args}`.)

Two empirical facts that correct/refine the design note:

1. **The bug needs BOTH GCM operands direct.** `overlapDetection` only walks att chains when
   `src.isDirect() && dst.isDirect()`. With the **JDK** Netty SSL provider, `SslEngineType.JDK.wantsDirectBuffer
   == false` (verified in netty-handler 4.1.130 bytecode: JDK ctor arg `iconst_0`; TCNATIVE/CONSCRYPT `iconst_1`),
   so Netty allocates **heap** wrap/unwrap destination buffers and the direct-direct branch is skipped => no CCE.
   Netty's `SslHandler.wrap` only force-converts a heap src to direct when `wantsDirectBuffer`; a src that is
   already direct is passed straight through. So the operands that matter are: any pre-existing **direct**
   BufferPool buffer that reaches wrap/unwrap.

2. **A pooled buffer only carries a non-Buffer att when it was sliced from a macro chunk.** `BufferPools.forNetworking()`
   gets in a fresh single-thread pool returned att=null in an ad-hoc probe (no Chunk/marker), so not every pooled
   buffer is a trigger — only sliced ones (the common case under real load) are. The canonical `BufferPool("test",
   8 MiB, true)` slices from one macro => att=Chunk/DirectBufferRef => reproduces.

**Where this leaves Option B.** The wrap **source** on the encrypt path is the `FrameEncoder.Payload` buffer, taken
directly from `BufferPools.forNetworking().getAtLeast(...)` and wrapped via `GlobalBufferPoolAllocator.wrap(frame)`
(FrameEncoderCrc.encode:90). Its `nioBuffer()` att chain reaches the sliced buffer's `Chunk`/`DirectBufferRef`.
So **swapping only `ChannelOption.ALLOCATOR` does not remove the wrap-source operand from BufferPool** — the frame
payload would still be a trigger whenever the SSL engine wants direct buffers (tcnative/conscrypt, or a JDK engine
fed a direct src). Pure "swap the allocator" is necessary for the unwrap-dest / unwrap-src / wrap-dest operands but
NOT sufficient for the wrap-source.

This is the decisive scoping finding: a complete, marker-free Option B must also move the **frame payload** to a
Netty buffer (or otherwise guarantee an att-legal wrap source), which reaches into `FrameEncoder.Payload` and the
`Wrapped.adopt()` ownership model used by inbound message assembly.

## Phase 2 — Prototype changes (what I changed and where)

1. `BufferPool.java` — added a test-only gate override `cassandra.test.bufferpool.gcm_marker` (validation hook to
   neutralise the marker; default behaviour unchanged).
2. `CassandraRelevantProperties.java` — new property `cassandra.net.netty_transport_allocator`
   (`CASSANDRA_NETTY_TRANSPORT_USES_NETTY_POOL`, default false).
3. `net/TransportByteBufAllocator.java` (new) — returns Netty's `PooledByteBufAllocator` when the flag is on, else
   `GlobalBufferPoolAllocator.instance`. Single shared Netty pool instance.
4. `net/InboundConnectionInitiator.java`, `net/OutboundConnectionInitiator.java`,
   `transport/PipelineConfigurator.java` — channel `ChannelOption.ALLOCATOR` now uses
   `TransportByteBufAllocator.instance()` instead of hard-wired `GlobalBufferPoolAllocator.instance`. Frame
   encoders/decoders deliberately left on `GlobalBufferPoolAllocator` (their `Payload`/`adopt()` ownership is
   BufferPool-based).
5. Tests: `net/SslHandlerGcmAllocatorTest.java` (JDK-GCM proof at the cipher level) and
   `net/TransportTlsRoundTripTest.java` (real Netty SslHandler TLS round trip over the transport allocator).

## Phase 3 — Validation results

### Build (req 1)
- JDK 11 (`11.0.31-amzn`): `build-test` BUILD SUCCESSFUL.
- JDK 25 (`25.0.3-amzn`): `build-test` BUILD SUCCESSFUL.
- `rm -rf build/classes build/test/classes` performed between JDK switches. NOTE: also delete `build/anttasks`
  between JDK switches — the `check-test-names` ant task is compiled by the active JDK and a JDK-25-compiled copy
  throws `UnsupportedClassVersionError` under JDK 11 (stale-artifact, not a code issue).
- Both new tests (`SslHandlerGcmAllocatorTest`, `TransportTlsRoundTripTest`) pass on JDK 11 and JDK 25.

### GCM correctness on JDK 25 (req 2) — marker neutralised throughout
- `BufferPoolTest.testAesGcmRoundTripWithPooledDirectBuffers` with
  `-Dtest.jvm.args=-Dcassandra.test.bufferpool.gcm_marker=false` => **ClassCastException** (reproduces the bug on
  JDK 25 when the marker is off). With the marker on (default) it passes. Confirms the marker is the current fix.
- `SslHandlerGcmAllocatorTest` (JDK 25):
  - `aesGcmWithNettyPooledBuffersWorksWithoutMarker` — AES-GCM on `PooledByteBufAllocator.nioBuffer()` operands:
    PASS regardless of marker. (Option B's buffers never need the marker.)
  - `aesGcmWithSlicedBufferPoolBuffersDependsOnMarker` — AES-GCM on sliced BufferPool operands: throws iff marker
    off on JDK 25, passes when on. PASS (adaptive assertion).
- `TransportTlsRoundTripTest.encryptedRoundTripUsesTransportAllocator` on JDK 25 with
  `-Dcassandra.net.netty_transport_allocator=true -Dcassandra.test.bufferpool.gcm_marker=false`: a real Netty
  `SslHandler` TLS 1.3 AES-GCM handshake + bidirectional app-data round trip over the Netty-pooled transport
  allocator => **PASS with the marker disabled**. (Skipped, by design, when Option B is off.)
- All of the above also pass on JDK 11.

**Conclusion (req 2): proven that, with the marker disabled, AES-GCM TLS works on JDK 25 because the operands are
Netty buffers.** The negative control (sliced BufferPool operands) fails on JDK 25 without the marker, confirming the
correctness comes from the Netty buffers, not a surviving marker.

### Safety audit (req 3) — and the BLOCKER it surfaced
Audited every `GlobalBufferPoolAllocator` consumer and the `Wrapped.adopt()` ownership path:
- **`FrameDecoder.channelRead` (net/FrameDecoder.java:266-278) requires the inbound `msg` to be a
  `BufferPoolAllocator.Wrapped`, else throws `IllegalArgumentException`.** It calls `.adopt()` to take ownership of
  the underlying BufferPool `ByteBuffer` and hands it to `allocator.putUnusedPortion(...)`/`ShareableBytes`.
- The inbound buffer reaching `FrameDecoder` is produced by the upstream handler: the **SslHandler's decrypted
  output**, allocated from `ctx.alloc()` = the channel allocator. Under Option B that is a Netty `PooledByteBuf`, NOT
  a `Wrapped`. So **pointing the internode/native channel allocator at Netty's pool breaks `FrameDecoder` with an
  `IllegalArgumentException` on the first inbound frame** (encrypted and unencrypted alike). This is used by both
  the internode `InboundMessageHandler` path and the native V5 `CQLMessageHandler` path.
- Same shape for streaming: `NettyStreamingChannel.send` and `AsyncStreamingOutputPlus` allocate frame buffers
  directly from `GlobalBufferPoolAllocator` and the inbound side adopts BufferPool buffers.

So the audit's answer to "does any non-TLS consumer pull a bare ByteBuffer out of a transport ByteBuf and free it via
BufferPool?" is **yes — the FrameDecoder/adopt path does, and it is hard-coded to assume BufferPool-backed buffers.**
This is the central blocker to a drop-in channel-allocator swap (and thus to marker removal) for internode + native
V5 + streaming.

The prototype's `TransportByteBufAllocator` flag therefore wires the SslHandler operands correctly (which is what
fixes GCM) but a complete implementation must also teach `FrameDecoder.channelRead` (and the streaming inbound path)
to accept arbitrary Netty `ByteBuf`s — copying/wrapping them into a `ShareableBytes` rather than `adopt()`-ing a
BufferPool buffer — and move the wrap-source `FrameEncoder.Payload` off the main pool. Those are the remaining
engineering items, not done in this prototype.

### Memory accounting (req 4) — what moves, and the operator implication
Today all transport buffers come from `BufferPools.forNetworking()` (the shared `BufferPool`), which is governed by a
single off-heap threshold (`networking_cache_size`, defaulting from `file_cache_size`/`getFileCacheSizeInMiB`) and
surfaced via `BufferPoolMetrics` (used/overflow/size). Under Option B, the buffers allocated by the **channel
allocator** — i.e. the SslHandler's per-connection wrap/unwrap (out-net + app) buffers and the inbound socket-read
buffers — move onto Netty's `PooledByteBufAllocator` arenas (`io.netty.allocator.*`: numDirectArenas, chunkSize=16
MiB default, per-`FastThreadLocal` caches), which are **not** counted against `networking_cache_size` and **not**
reflected in `BufferPoolMetrics`. The chunk cache and the entire SSTable/commitlog/compaction I/O stack stay on the
main `BufferPool`, untouched.

Operator implication: **two off-heap pools to reason about instead of one.** Total transport off-heap becomes
`BufferPool` (cache + I/O, and — in this prototype — still the frame payloads, since the frame encoders were left on
`BufferPool`) **plus** Netty arenas (the TLS/socket buffers). A single `file_cache_size`-style knob no longer bounds
transport; Netty arena sizing becomes a second tuning/observability surface. A production-quality Option B should
extend `BufferPoolMetrics` (or add metrics) to surface Netty `PooledByteBufAllocatorMetric` (arena/active/pinned
bytes) so operators keep one picture. Not solved here; documented as required follow-up.

### LongBufferPoolTest burn (req 5)
`ant burn-testsome -Dtest.name=org.apache.cassandra.utils.memory.LongBufferPoolTest` on JDK 25:
`Tests run: 2, Failures: 0, Errors: 0` (131.7 s). The end-of-run `TimeoutException`/"Got exception null" lines are
the test's normal time-budget shutdown (workers interrupted), not pool failures. The main `BufferPool` is undisturbed
by the prototype (the prototype does not modify pool internals beyond the test-only gate override).

## Phase 4 — Can the marker/map/gate be fully removed once transport is Netty-pooled?

**Not with this prototype, and not by the channel-allocator swap alone.** Three things must all hold before the
marker can be retired:

1. **Every wrap/unwrap GCM operand must be a Netty (att-legal) buffer.** The channel-allocator swap fixes the
   unwrap-src, unwrap-dst and wrap-dst. It does **not** fix the wrap-**source** frame payload
   (`FrameEncoder.Payload` / `GlobalBufferPoolAllocator.wrap`), nor the streaming send buffers
   (`NettyStreamingChannel`/`AsyncStreamingOutputPlus`), which are drawn straight from the main pool. Those remain
   BufferPool triggers whenever the SSL engine produces a direct dst (tcnative/conscrypt, or a JDK engine handed a
   direct src). => still need the marker, or move the payload to Netty too.
2. **`FrameDecoder.channelRead` (and the streaming inbound path) must accept non-`Wrapped` Netty buffers.** Today it
   throws `IllegalArgumentException` for anything but `BufferPoolAllocator.Wrapped`, so the swap breaks the live
   pipeline. => requires real code changes before the swap is even safe.
3. **§8 of the design note must hold: no non-TLS pooled buffer is ever a GCM operand.** Confirmed independently (TDE
   buffers are heap / not the main pool). So the *chunk-cache + I/O* pool never needs the marker for GCM. Good — once
   (1) and (2) are done for transport, the marker has no remaining GCM operand to protect and **could** be removed.

So: the marker is removable **in principle** once transport (channel allocator + frame payload + FrameDecoder/
streaming inbound) is fully Netty-pooled, but the prototype does not reach that bar. As shipped here it is an
*additive, flag-gated* change that fixes the SslHandler operands; the marker stays as the safety net (and is still
required for the wrap-source/streaming operands).

## Phase 5 — Open risks
1. **Memory accounting split** (above): transport off-heap leaves the unified `BufferPool` threshold/metrics for
   Netty arenas; operators get a second pool and tuning surface; `BufferPoolMetrics` no longer covers transport.
2. **Transport perf under TLS load**: `PooledByteBufAllocator` vs the current global pool on a TLS-heavy
   internode + native workload is unbenchmarked here. Arena defaults (chunkSize 16 MiB, per-thread caches) differ
   from the BufferPool's 128 KiB chunk / macro model; needs a soak/throughput+latency+RSS comparison before commit.
3. **(discovered) FrameDecoder/adopt coupling**: the channel-allocator swap actively breaks the inbound frame path
   until `FrameDecoder`/streaming-inbound are taught to accept Netty buffers — a larger change than "wire the
   allocator".

## GO / NO-GO

**NO-GO for a drop-in marker replacement on this branch; conditional GO for Option B as a tracked follow-up.**

Reasoning:
- The clean, principled outcome the design note hoped for ("swap the channel allocator, drop the marker") does **not**
  hold as a one-liner: (a) the wrap-source frame payload and streaming send buffers are BufferPool buffers
  independent of the channel allocator, so GCM operands are not all Netty buffers after the swap; and (b)
  `FrameDecoder.channelRead`/`adopt()` hard-require `BufferPoolAllocator.Wrapped` inbound and throw otherwise, so the
  naive swap breaks both the internode and native V5 pipelines (and streaming has the same shape).
- What **is** proven and valuable: Netty-pooled buffers make AES-GCM TLS work on JDK 25 with **no marker**
  (`SslHandlerGcmAllocatorTest`, `TransportTlsRoundTripTest`, marker forced off), validating the core Option B thesis
  at the SslHandler level. The prototype wires this behind `cassandra.net.netty_transport_allocator` (default off, so
  zero production change) and builds+passes on JDK 11 and JDK 25.
- Recommendation: **keep the shipped marker fix as-is for JDK 25 enablement.** Pursue Option B as a separate,
  properly scoped piece of work that also (1) moves `FrameEncoder.Payload` + streaming send buffers to Netty,
  (2) generalises `FrameDecoder`/streaming-inbound to accept Netty `ByteBuf`s, and (3) adds Netty-arena metrics +
  a TLS-load benchmark. Only after (1)-(3) can the marker/map/gate be removed. Until then Option B is additive, not
  a replacement.

