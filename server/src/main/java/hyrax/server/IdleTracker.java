package hyrax.server;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Watches for basking: a compute process holding GPU memory while its GPU does nothing.
 * Once that has lasted long enough it opens one alert per episode and screams.
 */
@Component
public class IdleTracker {

    private static final Logger log = LoggerFactory.getLogger(IdleTracker.class);

    public record GpuFlags(boolean reserved, Instant baskingSince) {}

    private record Process(String node, String gpuUuid, int pid) {}

    private record Owned(String node, int pid) {}

    private static final class Episode {
        final Instant idleSince;
        volatile Long alertId;

        Episode(Instant idleSince, Long alertId) {
            this.idleSince = idleSince;
            this.alertId = alertId;
        }
    }

    private final Map<Process, Episode> idle = new ConcurrentHashMap<>();
    private volatile Set<String> reservedGpus = Set.of();
    private volatile Set<Owned> owned = Set.of();
    private final ExecutorService slackQueue = Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon().factory());

    private final Alerts alerts;
    private final ObjectProvider<Scream> scream;
    private final JdbcTemplate jdbc;
    private final Duration alertAfter;
    private final Duration baskingAfter;
    private final int maxUtilization;
    private final long minMemoryMb;

    public IdleTracker(
            Alerts alerts,
            ObjectProvider<Scream> scream,
            JdbcTemplate jdbc,
            @Value("${hyrax.idle.alert-after}") Duration alertAfter,
            @Value("${hyrax.idle.basking-after}") Duration baskingAfter,
            @Value("${hyrax.idle.max-utilization}") int maxUtilization,
            @Value("${hyrax.idle.min-memory-mb}") long minMemoryMb) {
        this.alerts = alerts;
        this.scream = scream;
        this.jdbc = jdbc;
        this.alertAfter = alertAfter;
        this.baskingAfter = baskingAfter;
        this.maxUtilization = maxUtilization;
        this.minMemoryMb = minMemoryMb;
    }

    /** Picks up episodes that were open before a restart, so they are neither pinged twice nor left open. */
    @PostConstruct
    void resume() {
        for (Alerts.Alert alert : alerts.findOpen()) {
            idle.put(new Process(alert.node(), alert.gpuUuid(), alert.pid()), new Episode(alert.idleSince(), alert.id()));
        }
        refresh();
    }

    @PreDestroy
    void stop() {
        slackQueue.shutdown();
    }

    @Scheduled(fixedDelay = 60_000)
    void refresh() {
        reservedGpus = Set.copyOf(jdbc.queryForList("select uuid from gpu where reserved", String.class));
        owned = Set.copyOf(jdbc.query("select node, pid from process_owner",
                (row, index) -> new Owned(row.getString("node"), row.getInt("pid"))));
    }

    public void observe(Heartbeat heartbeat, Instant now) {
        try {
            track(heartbeat, now);
        } catch (DataAccessException error) {
            log.warn("Could not update alerts for {}: {}", heartbeat.node(), error.getMessage());
        }
    }

    private void track(Heartbeat heartbeat, Instant now) {
        String node = heartbeat.node();
        Set<Process> stillIdle = new HashSet<>();
        Set<Integer> pids = new HashSet<>();

        for (Heartbeat.Gpu gpu : heartbeat.gpus()) {
            if (gpu.uuid() == null || gpu.processes() == null) {
                continue;
            }
            boolean gpuIdle = gpu.utilization() != null && gpu.utilization() <= maxUtilization;
            for (Heartbeat.Process process : gpu.processes()) {
                pids.add(process.pid());
                boolean holdsMemory = "compute".equals(process.type())
                        && process.memoryMb() != null
                        && process.memoryMb() >= minMemoryMb;
                if (!gpuIdle || !holdsMemory) {
                    continue;
                }
                Process key = new Process(node, gpu.uuid(), process.pid());
                stillIdle.add(key);
                Episode episode = idle.computeIfAbsent(key, fresh -> new Episode(now, null));
                boolean overdue = !Duration.between(episode.idleSince, now).minus(alertAfter).isNegative();
                if (episode.alertId == null && overdue && !reservedGpus.contains(gpu.uuid())) {
                    open(episode, node, gpu, process, now);
                }
            }
        }

        for (Iterator<Map.Entry<Process, Episode>> entries = idle.entrySet().iterator(); entries.hasNext(); ) {
            Map.Entry<Process, Episode> entry = entries.next();
            if (entry.getKey().node().equals(node) && !stillIdle.contains(entry.getKey())) {
                entries.remove();
                if (entry.getValue().alertId != null) {
                    alerts.close(entry.getValue().alertId, now);
                }
            }
        }

        // A claim lasts as long as the process holds a GPU, so a reused pid is never misattributed.
        for (Owned claim : owned) {
            if (claim.node().equals(node) && !pids.contains(claim.pid())) {
                alerts.forgetOwner(claim.node(), claim.pid());
            }
        }
    }

    private void open(Episode episode, String node, Heartbeat.Gpu gpu, Heartbeat.Process process, Instant now) {
        var opened = alerts.open(node, gpu, process, episode.idleSince, now);
        if (opened.isEmpty()) {
            // Already open in the database; nothing to ping and nothing of ours to close.
            episode.alertId = 0L;
            return;
        }
        Alerts.Alert alert = opened.get();
        episode.alertId = alert.id();
        log.info("Alert {}: pid {} on {} GPU {} has held {} MiB idle since {}",
                alert.id(), alert.pid(), node, gpu.index(), alert.memoryMb(), alert.idleSince());
        send(alert);
    }

    @Scheduled(fixedDelay = 30_000)
    void remind() {
        for (Alerts.Alert alert : alerts.takeDueReminders(Instant.now())) {
            log.info("Alert {} is still open after its snooze", alert.id());
            send(alert);
        }
    }

    private void send(Alerts.Alert alert) {
        Scream slack = scream.getIfAvailable();
        if (slack == null) {
            log.info("Slack is not configured; alert {} was recorded but nobody was pinged", alert.id());
            return;
        }
        // Off the heartbeat thread: a slow Slack call must not delay the agent's round trip.
        slackQueue.execute(() -> slack.send(alert));
    }

    public GpuFlags flags(String gpuUuid, Instant now) {
        Instant baskingSince = idle.entrySet().stream()
                .filter(entry -> entry.getKey().gpuUuid().equals(gpuUuid))
                .map(entry -> entry.getValue().idleSince)
                .filter(since -> !Duration.between(since, now).minus(baskingAfter).isNegative())
                .min(Instant::compareTo)
                .orElse(null);
        return new GpuFlags(reservedGpus.contains(gpuUuid), baskingSince);
    }
}
