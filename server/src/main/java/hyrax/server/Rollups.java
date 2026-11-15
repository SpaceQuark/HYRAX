package hyrax.server;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Folds 3-second samples into one row per GPU per minute and stores them. */
@Component
public class Rollups {

    private static final Logger log = LoggerFactory.getLogger(Rollups.class);

    private record Key(String gpuUuid, Instant minute) {}

    private static final class Bucket {
        String node;
        String agentVersion;
        Heartbeat.Gpu latest;
        Instant lastSample;
        int samples;
        long utilizationSum;
        int utilizationSamples;
        int utilizationMax;
        long memoryUsedSum;
        long memoryUsedMax;
        long computeMemoryMax;

        synchronized void add(Heartbeat heartbeat, Heartbeat.Gpu gpu, Instant at) {
            node = heartbeat.node();
            agentVersion = heartbeat.agentVersion();
            latest = gpu;
            lastSample = at;
            samples++;
            if (gpu.utilization() != null) {
                utilizationSum += gpu.utilization();
                utilizationSamples++;
                utilizationMax = Math.max(utilizationMax, gpu.utilization());
            }
            memoryUsedSum += gpu.memoryUsedMb();
            memoryUsedMax = Math.max(memoryUsedMax, gpu.memoryUsedMb());
            long computeMemory = gpu.processes() == null ? 0 : gpu.processes().stream()
                    .filter(process -> "compute".equals(process.type()))
                    .map(Heartbeat.Process::memoryMb)
                    .filter(Objects::nonNull)
                    .mapToLong(Long::longValue)
                    .sum();
            computeMemoryMax = Math.max(computeMemoryMax, computeMemory);
        }
    }

    private final Map<Key, Bucket> buckets = new ConcurrentHashMap<>();
    private final JdbcTemplate jdbc;
    private final Duration retention;
    private final Set<String> reservedNodes;

    public Rollups(
            JdbcTemplate jdbc,
            @Value("${hyrax.sample-retention}") Duration retention,
            @Value("${hyrax.reserved-nodes:}") Set<String> reservedNodes) {
        this.jdbc = jdbc;
        this.retention = retention;
        this.reservedNodes = reservedNodes;
    }

    public void add(Heartbeat heartbeat, Instant at) {
        Instant minute = at.truncatedTo(ChronoUnit.MINUTES);
        for (Heartbeat.Gpu gpu : heartbeat.gpus()) {
            if (gpu.uuid() != null) {
                buckets.computeIfAbsent(new Key(gpu.uuid(), minute), key -> new Bucket()).add(heartbeat, gpu, at);
            }
        }
    }

    // A few seconds past the minute, so the minute being flushed has stopped receiving samples.
    @Scheduled(cron = "5 * * * * *")
    void flushFinishedMinutes() {
        flush(Instant.now().truncatedTo(ChronoUnit.MINUTES));
    }

    /** Stores every bucket for a minute earlier than {@code before}. A bucket that fails to store is kept and retried. */
    public void flush(Instant before) {
        for (Map.Entry<Key, Bucket> entry : buckets.entrySet()) {
            Key key = entry.getKey();
            if (!key.minute().isBefore(before)) {
                continue;
            }
            try {
                store(key, entry.getValue());
            } catch (DataAccessException error) {
                log.warn("Could not store telemetry rollups, will retry next minute: {}", error.getMessage());
                return;
            }
            buckets.remove(key);
        }
    }

    private void store(Key key, Bucket bucket) {
        synchronized (bucket) {
            Heartbeat.Gpu gpu = bucket.latest;
            boolean reservedByConfig = reservedNodes.contains(bucket.node);
            jdbc.update("""
                    insert into node (name, agent_version, last_seen) values (?, ?, ?)
                    on conflict (name) do update set
                        agent_version = excluded.agent_version,
                        last_seen = greatest(node.last_seen, excluded.last_seen)
                    """,
                    bucket.node, bucket.agentVersion, bucket.lastSample.atOffset(ZoneOffset.UTC));
            jdbc.update("""
                    insert into gpu (uuid, node, index, name, memory_total_mb, reserved, reason)
                    values (?, ?, ?, ?, ?, ?, ?)
                    on conflict (uuid) do update set
                        node = excluded.node, index = excluded.index,
                        name = excluded.name, memory_total_mb = excluded.memory_total_mb,
                        reserved = gpu.reserved or excluded.reserved,
                        reason = coalesce(gpu.reason, excluded.reason)
                    """,
                    key.gpuUuid(), bucket.node, gpu.index(), gpu.name(), gpu.memoryTotalMb(),
                    // Config can only add a reservation, so one set by hand in the database is never undone here.
                    reservedByConfig, reservedByConfig ? "node is listed in hyrax.reserved-nodes" : null);
            Float utilizationAvg = bucket.utilizationSamples == 0
                    ? null
                    : (float) bucket.utilizationSum / bucket.utilizationSamples;
            jdbc.update("""
                    insert into gpu_sample_1m (gpu_uuid, minute, samples, utilization_avg, utilization_max,
                        memory_used_avg_mb, memory_used_max_mb, compute_memory_max_mb)
                    values (?, ?, ?, ?, ?, ?, ?, ?)
                    on conflict (gpu_uuid, minute) do nothing
                    """,
                    key.gpuUuid(), key.minute().atOffset(ZoneOffset.UTC), bucket.samples,
                    utilizationAvg, utilizationAvg == null ? null : bucket.utilizationMax,
                    (float) bucket.memoryUsedSum / bucket.samples, bucket.memoryUsedMax, bucket.computeMemoryMax);
        }
    }

    @Scheduled(cron = "0 17 3 * * *")
    void purgeExpired() {
        int deleted = purge(Instant.now().minus(retention));
        log.info("Deleted {} telemetry rollups older than {}", deleted, retention);
    }

    public int purge(Instant olderThan) {
        return jdbc.update("delete from gpu_sample_1m where minute < ?", olderThan.atOffset(ZoneOffset.UTC));
    }
}
