package hyrax.server;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Latest heartbeat per node, in memory. Live samples are never persisted. */
@Component
public class ClusterState {

    public record Node(String node, String agentVersion, Instant lastSeen, boolean online, List<Heartbeat.Gpu> gpus) {}

    private record Entry(Heartbeat heartbeat, Instant receivedAt) {}

    private final Map<String, Entry> latest = new ConcurrentHashMap<>();
    private final Duration nodeTimeout;

    public ClusterState(@Value("${hyrax.node-timeout}") Duration nodeTimeout) {
        this.nodeTimeout = nodeTimeout;
    }

    public void record(Heartbeat heartbeat) {
        latest.put(heartbeat.node(), new Entry(heartbeat, Instant.now()));
    }

    public List<Node> snapshot() {
        Instant cutoff = Instant.now().minus(nodeTimeout);
        return latest.values().stream()
                .map(entry -> new Node(
                        entry.heartbeat().node(),
                        entry.heartbeat().agentVersion(),
                        entry.receivedAt(),
                        entry.receivedAt().isAfter(cutoff),
                        entry.heartbeat().gpus()))
                .sorted(Comparator.comparing(Node::node))
                .toList();
    }
}
