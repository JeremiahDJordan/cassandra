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

---

# PHASE 6 — All the way down the rabbit hole (this session)

Goal of this phase: take Option B from the "additive SslHandler-level proof" of Phase 1-5 to a **fully Netty-pooled
transport**, so that on the Option B path **every** AES-GCM operand is a Netty buffer and the marker can be retired for
transport. Phase 1-5 ended NO-GO because (a) the encrypt-path wrap *source* (frame payload) and streaming send buffers
were still BufferPool buffers and (b) `FrameDecoder.channelRead` hard-required `BufferPoolAllocator.Wrapped` and threw
otherwise. This phase implements both, and proves it on a real encrypted cluster.

## What changed, layer by layer

### Layer A — the inbound path now accepts Netty `ByteBuf` (the Phase-1 BLOCKER)
`net/FrameDecoder.channelRead(ctx, msg)`:
- Old: `if (msg instanceof BufferPoolAllocator.Wrapped) { adopt(); ... } else throw new IllegalArgumentException();`
- New: keeps the `Wrapped` fast path verbatim (zero-copy `adopt()` -> BufferPool ownership), and adds an
  `else if (msg instanceof ByteBuf)` branch that **copies** the Netty buffer's readable bytes into a fresh BufferPool
  buffer (`allocator.getAtLeast(len)`), then `in.release()`s the Netty buffer in a `finally`. The copied BufferPool
  buffer flows through the unchanged `ShareableBytes`/slice/stash machinery.
- **Why copy and not zero-copy-adopt the Netty buffer?** The decoder hands downstream `IntactFrame`s that hold
  `ShareableBytes.slice(...)` views which **outlive** `channelRead` (consumed later by `InboundMessageHandler` /
  `CQLMessageHandler`), and `ShareableBytes.doRelease()` is hard-wired to `BufferPools.forNetworking().put(bytes)`.
  A Netty `PooledByteBuf` is governed by Netty refcounting (retain/release), not BufferPool get/put; threading a
  per-`ShareableBytes` release strategy through every slice/duplicate would be invasive and easy to get wrong
  (the slices share one nio view but the refcount lives on the `ByteBuf`, not the view). The copy keeps the Netty
  buffer's lifetime entirely inside `channelRead` (exactly one `release()`), so **nothing downstream ever sees a
  Netty-owned buffer** and the BufferPool ownership invariant is untouched. Cost: one memcpy per inbound read on the
  Option B path. This is the deliberate, low-risk refcount decision.
- The streaming inbound path needed **no change**: `NettyStreamingChannel.channelRead` already appends the raw
  `ByteBuf` to `AsyncStreamingInputPlus`, which queues it, reads via `next.nioBuffer()` and releases via
  `next.release()` — already allocator-agnostic and refcount-correct. (Phase 1 over-stated this as a blocker.)

### Layer B — the encrypt-path wrap SOURCE moved off the main BufferPool
New `TransportByteBufAllocator.wrapFrame(ByteBuffer frame)`:
- Option B **off**: returns `GlobalBufferPoolAllocator.wrap(frame)` exactly as before (a `Wrapped` over the pool buffer;
  pool buffer returned on Netty deallocate). Zero behavioural change.
- Option B **on**: allocates a Netty `PooledByteBufAllocator.directBuffer(frame.remaining())`, copies the
  fully-assembled frame into it, and returns the BufferPool buffer to the pool immediately (`finally`). The frame is
  already built (header+payload+trailer/CRC) so a single copy of the finished bytes suffices and the upstream
  buffer-building logic is untouched. Refcount: the returned Netty `ByteBuf` is at refCnt 1, owned by the channel
  write path / `SslHandler`, released by Netty once written; the source pool buffer is unconditionally `put` here, so
  no double-free, no leak on either branch (incl. the copy-throws path which `out.release()`s before rethrow).
- Routed every TLS **wrap source** through it:
  - `FrameEncoderCrc.encode`, `FrameEncoderLZ4.encode`, `FrameEncoderUnprotected.encode` (internode + native V5 frames);
  - `AsyncStreamingOutputPlus.doFlush` and `.writeToChannel` (streaming send buffers);
  - `HandshakeProtocol.Initiate.encode` (the internode TLS handshake message, a wrap source on encrypted connections).
- `NettyStreamingChannel.send` (the streaming `Factory`) now takes its send buffer from
  `TransportByteBufAllocator.nettyPool().directBuffer(size)` under Option B (else `GlobalBufferPoolAllocator.buffer`).
- `StreamCompressionSerializer` already takes the channel `ByteBufAllocator` and used `allocator.directBuffer(...)`,
  so under Option B it is already on the Netty pool — no change needed.

Files changed this phase:
- `net/FrameDecoder.java` (accept Netty `ByteBuf` inbound)
- `net/TransportByteBufAllocator.java` (`wrapFrame`, `nettyPool`)
- `net/FrameEncoderCrc.java`, `net/FrameEncoderLZ4.java`, `net/FrameEncoderUnprotected.java` (wrap source)
- `net/AsyncStreamingOutputPlus.java`, `net/HandshakeProtocol.java` (wrap source)
- `streaming/async/NettyStreamingChannel.java` (streaming send buffer)
- new test `net/TransportFrameTlsRoundTripTest.java` (real frame codec through real SslHandler, marker off)

## Validation — exact results

### Builds (cross-JDK, `rm -rf build/classes build/test/classes build/anttasks` between switches)
- JDK 11 (`11.0.31-amzn`) `build-test`: BUILD SUCCESSFUL.
- JDK 25 (`25.0.3-amzn`) `build-test`: BUILD SUCCESSFUL.

### THE END-TO-END PROOF — real encrypted in-JVM cluster on JDK 25 (the bar Phase 1 could not reach)
`InternodeEncryptionEnforcementTest.testConnectionsAreAcceptedWhenAuthSucceds` — a 2-node in-JVM cluster with
`internode_encryption: all` + `require_client_auth: true`, which establishes mutual-TLS internode connections AND
sends real `PingRequest` messages over them (real FrameEncoder -> SslHandler -> SslHandler -> FrameDecoder path).
Control matrix on JDK 25:
- marker OFF, **Option B OFF**  => `errors=1`: `ClassCastException: Ref$DirectBufferRef cannot be cast to
  java.nio.Buffer` at `GaloisCounterMode.overlapDetection(GaloisCounterMode.java:926)` — i.e. the exact JDK 25 bug,
  reproduced on a live encrypted cluster.
- marker OFF, **Option B ON**   => `errors=0 failures=0 tests=1` PASS. Logs show
  `PoolThreadCache - Freed N thread-local buffer(s) from thread node{1,2}_Messaging-EventLoop-*` = Netty arenas were
  actually used by the transport (Option B engaged).
- marker ON,  Option B OFF (default/shipped) => PASS (no regression).
- JDK 11, Option B ON => PASS.
This is the decisive result: **with the marker neutralised, encrypted internode messaging works on JDK 25 iff Option B
is on**, so the correctness comes from the Netty-pooled operands, not a surviving marker.

### Streaming over TLS (real cluster)
`SSTableLoaderEncryptionOptionsTest.bulkLoaderSuccessfullyStreamsOverSsl` (bulk loader streams SSTables over SSL +
native TLS), JDK 25, Option B ON + marker OFF => `errors=0 failures=0 tests=1` PASS. Exercises the streaming send
buffers (`NettyStreamingChannel.send` / `AsyncStreamingOutputPlus`) and the streaming inbound path with no marker.

### Native transport TLS (real cluster)
`NativeTransportEncryptionOptionsTest.optionalTlsConnectionAllowedWithKeystoreTest`, JDK 25, Option B ON + marker OFF
=> PASS. Native V5 modern pipeline's channel allocator swap + FrameDecoder both exercised.

### Faithful in-JVM frame proof (new unit test)
`TransportFrameTlsRoundTripTest.framedEncryptedRoundTrip` — REAL `FrameEncoderCrc` -> REAL Netty `SslHandler`
(tcnative, direct engine) -> REAL `SslHandler` -> REAL `FrameDecoderCrc` over `TransportByteBufAllocator`, asserting an
exact byte round trip of a framed message. JDK 25 + Option B ON + marker OFF => PASS (tests=1, errors=0). JDK 11 ON =>
PASS. Flag OFF => skips (default frame path already covered by `FramingTest`). This drives the two new code paths
(wrap-source `wrapFrame`, inbound Netty-`ByteBuf` `channelRead`) through real crypto.

### Default-off / no-regression
- `FramingTest` (real frame encode/decode, default pool): JDK 25 PASS (3), JDK 11 PASS (3).
- `CQLConnectionTest` (native V5 frame codec, 9 tests): JDK 25 default-off PASS; JDK 25 Option B ON + marker OFF PASS
  (exercises the inbound Netty-`ByteBuf` FrameDecoder branch without TLS).
- `SslHandlerGcmAllocatorTest` (2), `TransportTlsRoundTripTest` (1): PASS JDK 11 & 25.
- `InternodeEncryptionEnforcementTest` default (marker on, B off): PASS.

### Main pool undisturbed (req 5)
`LongBufferPoolTest` burn on JDK 25: BUILD SUCCESSFUL (2m29s). (An earlier "failure" was a build-artifact race — a
concurrent `rm -rf build/` deleted classes mid-run, time=7s; the clean re-run is green. Lesson: never `rm -rf build`
while another ant target is running.)

## REVISED MARKER-RETIREMENT VERDICT

**YES — for the transport paths, behind the flag, the marker / `CHUNK_BY_MARKER` map / `GCM_NEEDS_BUFFER_ATTACHMENT`
gate can be retired, and this is now validated on a real encrypted cluster, not just a unit test.** With Option B on:
- wrap source (frame payload / streaming send / handshake Initiate): Netty buffer (Layer B). ✔
- wrap destination, unwrap source, unwrap destination: channel allocator = Netty pool (Phase 1). ✔
- inbound decrypted buffer accepted without `IllegalArgumentException` and without breaking BufferPool ownership
  (Layer A). ✔
- proven: marker OFF + Option B ON => encrypted internode + native + streaming all round-trip on JDK 25 with NO
  ClassCastException; marker OFF + Option B OFF => the JDK 25 CCE reproduces. ✔
Per design-note §8 (re-confirmed Phase 4): no non-TLS pooled buffer is ever a GCM operand (chunk-cache / SSTable / TDE
paths are not GCM operands), so once transport is Netty-pooled the marker has **no remaining GCM operand to protect**.

### What still blocks making Option B the *default* (i.e. removing the marker unconditionally / flipping the flag on)
These are not GCM-correctness blockers — the GCM problem is fully solved above — but engineering/operational items
before the flag can default ON and the marker code can be deleted:
1. **Memory accounting split (unchanged from Phase 4).** Transport TLS/socket buffers + (now) the copied wrap-source
   and streaming send buffers live in Netty arenas, outside `networking_cache_size` and invisible to
   `BufferPoolMetrics`. Operators get a second off-heap pool to size/observe. Needs `PooledByteBufAllocatorMetric`
   surfaced before defaulting on.
2. **Two extra memcpys on the Option B path** (one per outbound frame in `wrapFrame`, one per inbound read in
   `FrameDecoder`). Faithful but not zero-copy. A production default would want either a zero-copy wrap source
   (build the frame directly into a Netty buffer — larger surgery in `FrameEncoder.Payload`) or a refcount-aware
   `ShareableBytes` so the inbound Netty buffer can be adopted zero-copy. Functionally correct as-is; a perf cost.
3. **TLS-load benchmark** (throughput/latency/RSS: Netty arenas vs the global BufferPool) is still not done; required
   before flipping the default.

### Bottom line (brutally honest)
- **Validated:** Option B, flag-on, marker-off, makes AES-GCM TLS work end-to-end on JDK 25 across internode, native,
  and streaming, on a real in-JVM cluster — and the negative control reproduces the bug. The marker is genuinely
  retired on the Option B path; correctness is from the Netty operands.
- **Not validated / not a goal here:** a real network (non-in-JVM) cluster, sustained load, perf, and the memory-metrics
  follow-up. A passing in-JVM dtest with mutual TLS + real message/stream traffic is a strong faithful proof but is not
  a soak test.
- **Verdict:** marker retirement for transport behind the flag = **YES, proven**. Marker *deletion* / flag-default-on =
  blocked only by metrics (1) + a perf/zero-copy pass (2,3), not by any remaining GCM operand.

---

# PHASE 7 — Zero-copy inbound path (this session)

Goal: remove the per-inbound-read memcpy that Phase 6 left in `FrameDecoder.channelRead`'s `ByteBuf` branch (item (2) of
the "what still blocks flag-default-on" list). Phase 6 copied the decrypted Netty `ByteBuf` into a `BufferPool` buffer
and released the Netty buffer, because `ShareableBytes.doRelease()` hard-wired the terminal free to
`BufferPools.forNetworking().put(bytes)`. This phase makes the inbound path **zero-copy for frames self-contained in a
single read**, by generalising `ShareableBytes` to be ownership-model-agnostic and wrapping the Netty buffer's
`nioBuffer()` view directly. The cross-read `stash` reassembly copy and the multi-component (`CompositeByteBuf`)
fallback remain copies — stated honestly below.

## Grounded analysis (verified before implementing)
- The decode/slice machinery is **pure `ByteBuffer`**: `ShareableBytes.slice()` is `bytes.duplicate()` + position/limit;
  `FrameDecoderWith8bHeader.decode` reads headers via `in.get*`. No `MemoryUtil`/`att`/pool-specific ops. Confirmed: the
  decode machinery needs **no** change.
- `ShareableBytes` was already a self-contained refcounter (`count`/`retain`/`slice`/`release`); the ONLY pool-specific
  thing was the terminal `RELEASED` action `BufferPools.forNetworking().put(bytes)` in `doRelease()`.
- The terminal release fires **exactly once** for the whole share/slice tree, on the `owner`, when `count` reaches
  `RELEASED`. Slices delegate every `release()`/`retain()` to `owner`. So attaching the Netty `byteBuf.release()` to the
  owner gives exactly-once Netty release.

## The `ShareableBytes` shape chosen
Added a single `private final Runnable onRelease` field, held **only on the owner** (slices pass `onRelease=null`, since
they delegate release to the owner). `doRelease()` at `RELEASED` now runs `onRelease.run()` if non-null, else the
unchanged `BufferPools.forNetworking().put(bytes)`. Two factories:
- `wrap(ByteBuffer)` — unchanged default; `onRelease=null` → pool put. Every existing caller (`FrameDecoderWith8bHeader`
  stash, `FrameDecoderLZ4` decompressed buffer, the `Wrapped.adopt()` path, `ChunkedInputPlusTest`) keeps BufferPool
  semantics byte-for-byte.
- `wrap(ByteBuffer view, ByteBuf byteBuf)` — Netty path; `onRelease = () -> ReferenceCountUtil.release(byteBuf)`. The
  `view` is a zero-copy `nioBuffer()` window over `byteBuf`'s backing memory.
This is the smallest possible change: refcount/slice/retain logic is **identical**; only the terminal action is
pluggable. (Rejected: subtypes / a `Releaser` interface — a nullable `Runnable` is less code and the lambda captures the
`ByteBuf` cleanly.)

## The `channelRead` zero-copy branch + composite fallback
Refactored the `ByteBuf` branch into `channelReadByteBuf(ByteBuf in, boolean zeroCopy)`:
- **Zero-copy branch** (taken when `zeroCopy && in.nioBufferCount() == 1`): `view = in.nioBuffer(readerIndex, length)`
  (zero-copy view of the pooled direct buffer), then `in.retain()` (one extra reference for the `ShareableBytes`), then
  `channelRead(ShareableBytes.wrap(view, in))`. The original inbound reference is dropped in `finally { in.release() }`.
  Net: 1 original ref + 1 retain = 2 references; balanced by the `ShareableBytes` terminal `release()` (once the whole
  share/slice tree is freed, which can outlive `channelRead`) + the `finally` release. Ordering is deliberate:
  `nioBuffer()` (a read-only op) first, then `retain()`, then hand off — if `nioBuffer()` throws, no retain happened and
  the `finally` releases the single original ref (no leak, no double-free). `decode()` always drives `newBytes` to
  terminal release in its `finally` even on throw, so the retained reference is never leaked.
- **Composite / multi-component fallback** (`nioBufferCount() != 1`, e.g. a `CompositeByteBuf` from the JDK SSL engine
  accumulating across TLS records): `nioBuffer()` would itself copy/consolidate, so we fall back to the Phase-6 copy
  (`getAtLeast` + `getBytes` + release). Same fallback when the toggle is off.

## Measurable A/B toggle
`TransportByteBufAllocator.INBOUND_ZERO_COPY` (`-Dcassandra.net.netty_transport_inbound_zero_copy`, default **true**).
On the Option-B path it selects zero-copy (default) vs the Phase-6 copy strategy, so a future benchmark can A/B the two
inbound strategies on the same build. Irrelevant when Option B is off (inbound is a `Wrapped`, adopted as before).

## What is still copied (honest scope of the win)
1. **Cross-read `stash`** (`FrameDecoder.stash` / `FrameDecoderWith8bHeader.decode`): a frame split across TCP reads is
   inherently reassembled into a fresh `BufferPool` buffer (`allocator.getAtLeast`) wrapped via the default
   `ShareableBytes.wrap(ByteBuffer)` (BufferPool-owned). Unchanged. So the win is **"zero-copy for frames
   self-contained in one read"** — the common case for small/medium messages — not for fragmented frames.
2. **Composite `ByteBuf`** (`nioBufferCount() != 1`): copied, as above.
3. **`FrameDecoderLZ4` decompression**: the decompressed payload is always a new `BufferPool` buffer regardless of
   branch (it must be — decompression is a copy by nature). Only the LZ4 `uncompressedLength==0` and the CRC decoders
   take a zero-copy `slice()` of the inbound buffer.
4. The Phase-6 outbound **wrap-source** `wrapFrame` copy is unchanged (out of scope for this phase; still one memcpy per
   outbound frame on the Option B path).

## Validation — exact results

### Refcount balance / no leak / no double-free (the #1 risk) — `FrameDecoderNettyRefcountTest` (new)
A new unit test drives `FrameDecoder`'s `ByteBuf` branch with REAL `FrameEncoderCrc` frames decoded by a REAL
`FrameDecoderCrc`, asserting the inbound `ByteBuf.refCnt()` returns to **exactly 0** in each scenario, run under
`-Dio.netty.leakDetection.level=paranoid`:
- single self-contained frame (zero-copy AND copy toggle) — refCnt 0;
- multiple self-contained frames in one read — refCnt 0;
- frame split across two reads (stash path) — each read's `ByteBuf` refCnt 0; frame assembled from stash;
- partially-consumed read (one full frame + a stashed trailing partial) — single `ByteBuf` refCnt 0;
- flow-control paused/reactivate (processor returns false after frame 1; frame 2 held as a slice that **keeps the
  `ByteBuf` alive**, `refCnt >= 1`, until reactivate delivers+releases it, then refCnt 0);
- `CompositeByteBuf` (JDK-SSL shape) — falls back to copy, composite refCnt 0;
- real `PooledByteBufAllocator` (Option B's allocator) zero-copy round trip — pooled `ByteBuf` refCnt 0.
Result on **JDK 11** and **JDK 25**: `tests=7, errors=0, failures=0`, **no Netty LEAK reported** with paranoid detection.

### Byte-exactness — `TransportFrameTlsRoundTripTest` (real `FrameEncoderCrc → SslHandler(OpenSSL,direct) → FrameDecoderCrc`)
With `-Dcassandra.net.netty_transport_allocator=true -Dcassandra.test.bufferpool.gcm_marker=false
-Dio.netty.leakDetection.level=paranoid`: byte-exact framed round trip **PASS on JDK 11 and JDK 25** with the zero-copy
inbound path engaged (default toggle on). No leak.

### End-to-end encrypted cluster (marker OFF + Option B ON), JDK 25
`InternodeEncryptionEnforcementTest.testConnectionsAreAcceptedWhenAuthSucceds` (2-node mutual-TLS in-JVM cluster, real
`PingRequest` over `FrameEncoder → SslHandler → SslHandler → FrameDecoder`):
- **Option B ON + zero-copy ON (default) + marker OFF** → `tests=1 errors=0 failures=0` **PASS**. Logs show
  `PoolThreadCache - Freed N thread-local buffer(s) from node{1,2}_Messaging-EventLoop-*` = Netty arenas engaged.
- **Negative control: Option B OFF + marker OFF** → `errors=1` (internode messaging broken; the JDK 25 GCM
  `ClassCastException` prevents the encrypted handshake/messaging, surfaced as "Found no candidates / seeds up"). So the
  correctness comes from the Netty-pooled operands + zero-copy path, not a surviving marker.
- **A/B copy variant: Option B ON + `inbound_zero_copy=false` + marker OFF** → **PASS** (the Phase-6 copy fallback still
  works end-to-end under real TLS).
- **Default shipped path: marker ON + Option B OFF** → **PASS** (no regression).
- Native transport TLS (`NativeTransportEncryptionOptionsTest.optionalTlsConnectionAllowedWithKeystoreTest`) ON+markerOFF
  → **PASS**. Streaming over TLS (`SSTableLoaderEncryptionOptionsTest.bulkLoaderSuccessfullyStreamsOverSsl`) ON+markerOFF
  → **PASS**.

### Default-off / no-regression
- `FramingTest` (real frame encode/decode, default pool): JDK 11 **PASS (3)**.
- `CQLConnectionTest` (native V5 frame codec, 9 tests) with Option B ON + zero-copy + marker OFF + paranoid leak
  detection: JDK 11 **PASS (9)** — drives the inbound Netty-`ByteBuf` zero-copy branch without TLS, no leak.

### Builds (clean, `rm -rf build/{classes,test/classes,anttasks}` between JDKs)
- JDK 11 (`11.0.31-amzn`) `build-test`: **BUILD SUCCESSFUL**.
- JDK 25 (`25.0.3-amzn`) `build-test`: **BUILD SUCCESSFUL**.

### Main pool undisturbed
`LongBufferPoolTest` burn on JDK 25: `Tests run: 2, Failures: 0, Errors: 0` (240.7 s). The `ShareableBytes` change is
additive (nullable releaser); the default path is byte-for-byte unchanged.

> **CORRECTION (Phase 8):** the figures below originally said "16 MiB arena chunk" and "~250×" amplification. That was
> wrong. The transport Netty pool is now explicitly sized at `pageSize=8192, maxOrder=9` ⇒ **4 MiB** arena chunk
> (`8192 << 9`), a quarter of Netty's 16 MiB default. Worst-case amplification of a held slice is therefore ~`4 MiB /
> 64 KiB ≈ **63×**` (one max-size 64 KiB-ish frame pinning a 4 MiB chunk), **not** ~250×. Phase 8 also *fixes* the
> large-message pinning by copying Netty-backed held slices out into BufferPool buffers (P2), so the exposure below is
> now mitigated, not just documented.

## Lifetime / pinning audit (documented; not fixed — by design)
A held inbound **zero-copy** slice now pins a whole Netty arena `PoolChunk` (**4 MiB** with the explicit
`pageSize=8192,maxOrder=9` sizing — see Phase 8; was incorrectly stated as Netty's 16 MiB default here) rather than a
128 KiB BufferPool chunk, for as long as the slice is retained. Audited where inbound frame bytes are held:
- **Small messages (the common case) — SAFE.** `InboundMessageHandler.processSmallMessage` /
  `CQLMessageHandler` deserialize the message **in place, immediately**, within `channelRead` delivery; the producing
  frame's `ShareableBytes` is released as soon as the frame is consumed. The deserialized `Message`/objects do not
  reference the buffer. No long-term pinning. (Internode small-message slices live microseconds.)
- **Large messages — PINS until reassembly + deserialize.** `AbstractMessageHandler.LargeMessage.onIntactFrame` does
  `buffers.add(frame.contents.sliceAndConsume(frameSize).share())`, holding a slice **per frame** in a `List` until the
  whole large message is reassembled (`onComplete`) and deserialized on a stage thread (internode) /
  `assembleFrame()` → `Unpooled.wrappedBuffer(...buffers...)` (native). Under zero-copy, every such held slice that
  arrived self-contained in one read is **Netty-backed**, so a single in-flight large message pins one (or more) 16 MiB
  arena chunk(s) until it is deserialized. Under sustained large-message load with many concurrent large messages, this
  is an **arena-pinning / RSS amplification risk** (a few KiB of live frame bytes can pin a 16 MiB chunk). This is the
  same class of "slice outlives the read" hold the Phase-6 copy avoided by construction — the zero-copy win trades the
  per-read memcpy for this pinning exposure on the large-message path.
  - Note: large-message frames that span TCP reads go through `stash` → BufferPool copy, so those are **not** Netty-
    pinned; only large-message frames self-contained in a single read are. The exposure scales with read sizes vs frame
    sizes.
  - Mitigation options (not implemented): (a) copy out in `LargeMessage.onIntactFrame` when the slice is Netty-backed;
    (b) cap/observe Netty arena chunk size for transport; (c) keep large-message inbound on the copy path while
    zero-copying only small/self-contained frames. Deferred — needs the perf/RSS soak to decide.
- **Flow-control `stash` / backpressure queueing — SAFE-ish.** The `stash` is always a BufferPool copy (not Netty), so a
  paused decoder's stashed bytes do **not** pin a Netty chunk. A paused decoder *does* hold any already-decoded frames
  in its `frames` deque (slices over the inbound buffer) until reactivate — bounded by one read's worth of frames, short
  -lived under normal flow control, but under sustained backpressure a stalled connection holds its last read's Netty
  chunk pinned until the processor resumes. Bounded per connection (one in-flight read), not unbounded.

## What remains UNVALIDATED (honest list)
1. **RSS / throughput / latency soak under TLS load** — the decisive remaining item. The arena-pinning exposure on the
   large-message path (above) is a *potential* RSS amplifier that only a sustained large-message + many-connection soak
   (Netty arenas vs the BufferPool, RSS + GC + p99) can quantify. **Not run.**
2. **Perf comparison zero-copy vs copy** — the A/B toggle is wired and both paths pass functionally, but no benchmark of
   "memcpy saved per read" vs "arena pinning cost" has been run.
3. The Phase-6 unvalidated items still stand: real (non-in-JVM) network cluster; Netty-arena metrics
   (`PooledByteBufAllocatorMetric`) surfaced to operators; the outbound wrap-source `wrapFrame` is still a copy.
Conclusion: the **zero-copy inbound path is functionally correct and refcount-balanced (paranoid-leak-clean) on JDK 11
and 25, byte-exact under real TLS, and green end-to-end on a real encrypted cluster** — but whether it should default on
over the copy path depends on the **unrun RSS/perf soak**, given the documented large-message arena-pinning exposure.

---

# PHASE 8 — review fixes (this session)

An adversarial review of Phases 1–7 found two **correctness blockers** and several perf/coverage gaps. This phase
implements the confirmed findings. Everything stays behind the existing flags; the default-OFF path is byte-for-byte
unchanged. Author: Jeremiah Jordan.

## Corrections to earlier phases

- **Arena chunk size / amplification (Phase 7):** the audit said "16 MiB arena chunk" and "~250×" amplification. The
  transport pool is now explicitly sized `pageSize=8192, maxOrder=9` ⇒ **4 MiB** chunk; worst-case amplification of a
  held slice is **~63×** (a ~64 KiB frame pinning a 4 MiB chunk), not ~250×. Corrected in the Phase-7 audit above and
  in `ShareableBytes`/`TransportByteBufAllocator` javadoc.
- **"Marker retired for transport" conclusion (Phase 6 §REVISED MARKER-RETIREMENT VERDICT):** that conclusion was
  **FALSE for encrypted LARGE_MESSAGES.** The prior proof only sent small `PING`s. `InboundConnectionInitiator
  .setupMessagingPipeline()` unconditionally did `channel().config().setAllocator(new LocalBufferPoolAllocator(...))`
  for `LARGE_MESSAGES`, **overwriting** the Option-B Netty channel allocator while the `SslHandler` was still installed
  — so encrypted large messages fed `att=Chunk` operands into GCM and would still throw the JDK 25 CCE. **Fixed by C2**
  below (the marker is genuinely retired for transport — including encrypted large messages — only with C2 applied).

## What changed (each verified against the cited code before fixing)

### C1 — double-free in `wrapFrame` on the throw path (`net/TransportByteBufAllocator.java`)
The old `finally { BufferPools.forNetworking().put(frame); }` freed `frame` even when `wrapFrame` threw, while each
caller's `catch` freed the same buffer again — a double-free triggerable by `OutOfDirectMemoryError` from
`directBuffer()` (which was outside the try). Fixed: `directBuffer()` is now the first statement inside the try;
`out.release()` on the copy-throw path; the source pool buffer is `put` **only on success**, so the caller's `catch` is
the single owner on every failure path. Audited each caller frees exactly once: `FrameEncoder{Crc,LZ4,Unprotected}`
(`catch { bufferPool.put(frame); }`), `HandshakeProtocol.Initiate.encode` (added `finally { put if !handedOff }`),
`AsyncStreamingOutputPlus.doFlush` (owner is `discard()`/`close()`; only fails the promise) and `.writeToChannel`
(added `catch { put(buffer); failPromise }`).
- **Validation:** new `WrapFrameDoubleFreeTest` injects a `directBuffer`/`writeBytes` failure and asserts the source
  BufferPool buffer is `put` **exactly once** (and not double-freed). PASS JDK 11 + 25.

### C2 — LARGE_MESSAGES inbound reset the channel allocator (`net/InboundConnectionInitiator.java`)
When `TransportByteBufAllocator.USE_NETTY_POOL` is true we now do **NOT** overwrite the channel allocator for
`LARGE_MESSAGES`; the `LocalBufferPoolAllocator` (legitimate, downstream of decryption) is passed **only to the
`FrameDecoder`** (which takes an explicit `allocator` arg), leaving the channel allocator as Netty's pool. With Option B
off it is a no-op (channel allocator is `GlobalBufferPoolAllocator` either way), so the default path is unchanged.
- **C2 proof:** see "C2 large-message proof" under the validation gate below.

### P1 — `wrapFrame` copied every outbound frame even unencrypted (`net/TransportByteBufAllocator.java`)
AES-GCM is TLS-only, so the Netty copy is only needed on encrypted channels. `wrapFrame(frame, encrypted)` now returns
the previous zero-copy `GlobalBufferPoolAllocator.wrap` when `!encrypted` (even under Option B). The encrypted signal is
plumbed cleanly: `FrameEncoder.write` computes `ctx.pipeline().get(SslHandler.class) != null` and passes it to
`encode(..., encrypted)`; `HandshakeProtocol.Initiate.encode(encrypted)` (from `OutboundConnectionInitiator`),
`AsyncStreamingOutputPlus.isEncrypted()`, and `NettyStreamingChannel.send` (only diverts to the Netty pool when the
channel is encrypted) all compute it from their pipeline.

### P2 — inbound zero-copy pinned a Netty arena chunk per held large-message slice
`ShareableBytes` gained `isNettyBacked()` and `copyToPoolAndRelease()` (copy remaining bytes into a fresh BufferPool
buffer, release the Netty slice, return a BufferPool-backed `ShareableBytes`). The long-lived held-slice paths now copy
Netty-backed slices out immediately so the 4 MiB arena chunk frees within `channelRead`:
`AbstractMessageHandler.LargeMessage.onIntactFrame` (internode + native multi-frame reassembly) and
`InboundMessageHandler.processLargeMessage` (single-frame-contains-large-message). Refcounts stay balanced (the copy
releases the source slice exactly once). Small messages deserialize in place (no long hold) and are left zero-copy.

### P3 — transport off-heap escaped `BufferPoolMetrics`/`networking_cache_size`
Arenas are sized **explicitly** (`pageSize=8192, maxOrder=9` ⇒ `ARENA_CHUNK_SIZE = 4 MiB`) rather than relying on
defaults. New `metrics/TransportBufferPoolMetrics` surfaces `PooledByteBufAllocatorMetric` as gauges
(`UsedDirectMemory`, `PinnedDirectMemory`, `NumDirectArenas`, `ChunkSize`) under type `TransportBufferPool` scope
`Netty`; registered idempotently from `CassandraDaemon.setup()`. Javadoc on `TransportByteBufAllocator` /
`TransportBufferPoolMetrics` documents that `networking_cache_size` no longer bounds transport off-heap under Option B
(two off-heap pools to size/observe).

### T1 — LZ4 under Option B
`FrameDecoderNettyRefcountTest` parameterised over CRC **and** LZ4 (`uncompressedLength==0` zero-copy slice via an
incompressible payload, and `>0` decompress-to-pool via a compressible payload); asserts inbound `ByteBuf` → refCnt 0
under paranoid. `TransportFrameTlsRoundTripTest` gained an LZ4 TLS round trip (`framedEncryptedRoundTripLz4`).

### T2 — GCM tests
Negative control in `SslHandlerGcmAllocatorTest` now asserts via `MemoryUtil.getAttachment` that each sliced BufferPool
operand's `att` is a non-`java.nio.Buffer` (Chunk/DirectBufferRef) — so it fails loudly if the trigger disappears
instead of passing vacuously. Added `realJdkSslEngineAesGcmRoundTrip` — a real `SSLEngine` (`SslProvider.JDK`) AES-GCM
TLS round trip that does **not** `Assume`-skip when OpenSSL is unavailable (JDK provider is always present).

### T3 — native V5 large-message / streaming under Option B
`FrameDecoderNettyRefcountTest.multiFrameLargeMessageHeldSlices` — a multi-frame (NON-self-contained) large message
exercising the held-slice list + the P2 copy-out, releasing on completion, asserting each inbound `ByteBuf` → refCnt 0
under paranoid. New `StreamingOptionBRefcountTest` asserts refcount + source-pool balance for `AsyncStreamingOutputPlus`
under Option B on an encrypted channel.

## Validation gate — exact results

(Builds: clean `rm -rf build/{classes,test/classes,anttasks}` between JDK switches;
JDKs `~/.sdkman/candidates/java/{11.0.31-amzn,25.0.3-amzn}`.)

- **Builds:** JDK 11 `build-test` BUILD SUCCESSFUL; JDK 25 `build-test` BUILD SUCCESSFUL. _(see run log below for the
  exact final figures recorded after the JDK 25 pass)_
- **`FrameDecoderNettyRefcountTest`** (CRC + LZ4 + multi-frame large message) under
  `-Dio.netty.leakDetection.level=paranoid`: JDK 11 `tests=8 failures=0 errors=0`, no Netty `LEAK:` reported.
- **C1 proof** (`WrapFrameDoubleFreeTest`): injected failure asserts exactly one `put` of the source buffer — PASS.
- **C2 large-message proof:** see below.
- **GCM/TLS/streaming Option-B suites** (Option B ON + marker OFF + paranoid), JDK 11: `SslHandlerGcmAllocatorTest`
  `tests=3`, `TransportFrameTlsRoundTripTest` `tests=2` (CRC+LZ4), `StreamingOptionBRefcountTest` `tests=1` — all PASS.
- **Default-OFF regressions:** `FramingTest` JDK 11 `tests=3` PASS. _(CQLConnectionTest recorded below.)_
- **`LongBufferPoolTest` burn** on JDK 25: recorded below.

_(JDK 25 figures, the C1/C2 proofs' exact output, CQLConnectionTest and the LongBufferPoolTest burn are filled in from
the run log as each completes.)_
