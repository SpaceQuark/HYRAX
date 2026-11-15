package hyrax.server;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Latest heartbeat per node, in memory. Live samples are never persisted. */
@Component
public class ClusterState {

    /** {@code flags} holds, per GPU UUID, what the server knows beyond the agent's reading. */
    public record Node(
            String node,
            String agentVersion,
            Instant lastSeen,
            boolean online,
            List<Heartbeat.Gpu> gpus,
            Map<String, IdleTracker.GpuFlags> flags) {}

    private record Entry(Heartbeat heartbeat, Instant receivedAt) {}

    private final Map<String, Entry> latest = new ConcurrentHashMap<>();
    private final Duration nodeTimeout;
    private final IdleTracker idleTracker;

    public ClusterState(@Value("${hyrax.node-timeout}") Duration nodeTimeout, IdleTracker idleTracker) {
        this.nodeTimeout = nodeTimeout;
        this.idleTracker = idleTracker;
    }

    public void record(Heartbeat heartbeat) {
        latest.put(heartbeat.node(), new Entry(heartbeat, Instant.now()));
    }

    public List<Node> snapshot() {
        Instant now = Instant.now();
        Instant cutoff = now.minus(nodeTimeout);
        return latest.values().stream()
                .map(entry -> new Node(
                        entry.heartbeat().node(),
                        entry.heartbeat().agentVersion(),
                        entry.receivedAt(),
                        entry.receivedAt().isAfter(cutoff),
                        entry.heartbeat().gpus(),
                        entry.heartbeat().gpus().stream()
                                .filter(gpu -> gpu.uuid() != null)
                                .collect(Collectors.toMap(
                                        Heartbeat.Gpu::uuid, gpu -> idleTracker.flags(gpu.uuid(), now), (a, b) -> a))))
                .sorted(Comparator.comparing(Node::node))
                .toList();
    }
}
