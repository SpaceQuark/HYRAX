package hyrax.server;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api")
public class ApiController {

    private final ClusterState cluster;
    private final LiveStream stream;
    private final String agentToken;

    public ApiController(ClusterState cluster, LiveStream stream, @Value("${hyrax.agent-token}") String agentToken) {
        this.cluster = cluster;
        this.stream = stream;
        this.agentToken = agentToken;
    }

    @PostMapping("/agent/heartbeat")
    public Heartbeat.Desired heartbeat(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody Heartbeat heartbeat) {
        requireAgent(authorization);
        if (heartbeat.node() == null || heartbeat.node().isBlank() || heartbeat.gpus() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "node and gpus are required");
        }
        cluster.record(heartbeat);
        stream.publish(cluster.snapshot());
        // No scheduler yet, so no node is asked to run or cancel anything.
        return new Heartbeat.Desired(List.of(), List.of());
    }

    @GetMapping("/nodes")
    public List<ClusterState.Node> nodes() {
        return cluster.snapshot();
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return stream.subscribe(cluster.snapshot());
    }

    private void requireAgent(String authorization) {
        if (agentToken.isBlank()) {
            return;
        }
        byte[] expected = ("Bearer " + agentToken).getBytes(StandardCharsets.UTF_8);
        byte[] actual = (authorization == null ? "" : authorization).getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, actual)) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
    }
}
