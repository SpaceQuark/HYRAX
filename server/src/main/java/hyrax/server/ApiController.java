package hyrax.server;

import java.time.Instant;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
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

    private static final Logger log = LoggerFactory.getLogger(ApiController.class);

    private final ClusterState cluster;
    private final LiveStream stream;
    private final Rollups rollups;
    private final IdleTracker idleTracker;
    private final Auth auth;
    private final Scheduler scheduler;
    private final Jobs jobs;

    public ApiController(
            ClusterState cluster,
            LiveStream stream,
            Rollups rollups,
            IdleTracker idleTracker,
            Auth auth,
            Scheduler scheduler,
            Jobs jobs) {
        this.rollups = rollups;
        this.idleTracker = idleTracker;
        this.cluster = cluster;
        this.stream = stream;
        this.auth = auth;
        this.scheduler = scheduler;
        this.jobs = jobs;
    }

    @PostMapping("/agent/heartbeat")
    public Heartbeat.Desired heartbeat(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody Heartbeat heartbeat,
            HttpServletRequest request) {
        if (heartbeat.node() == null || heartbeat.node().isBlank() || heartbeat.gpus() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "node and gpus are required");
        }
        boolean mayRunJobs = auth.agentMayRunJobs(authorization, heartbeat.node(), request);
        cluster.record(heartbeat);
        Instant now = Instant.now();
        rollups.add(heartbeat, now);
        idleTracker.observe(heartbeat, now);
        scheduler.observe(heartbeat, now);
        stream.publish(cluster.snapshot());
        if (!mayRunJobs) {
            return Heartbeat.Desired.nothing();
        }
        try {
            return jobs.reconcile(heartbeat.node(), heartbeat.attempts());
        } catch (DataAccessException error) {
            // An empty reply asks the agent to change nothing: it only stops an attempt it is told to cancel.
            log.warn("Could not reconcile jobs for {}: {}", heartbeat.node(), error.getMessage());
            return Heartbeat.Desired.nothing();
        }
    }

    @GetMapping("/nodes")
    public List<ClusterState.Node> nodes() {
        return cluster.snapshot();
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return stream.subscribe(cluster.snapshot());
    }
}
