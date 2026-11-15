package hyrax.server;

import java.util.List;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** Jobs for people, enrollment for admins, log upload for agents. All of it HTTPS only. */
@RestController
@RequestMapping("/api")
public class JobController {

    public record NewUser(String name, boolean admin) {}

    private final Auth auth;
    private final Jobs jobs;
    private final JdbcTemplate jdbc;
    private final long maxLogBytes;

    public JobController(Auth auth, Jobs jobs, JdbcTemplate jdbc, @Value("${hyrax.jobs.max-log-bytes}") long maxLogBytes) {
        this.auth = auth;
        this.jobs = jobs;
        this.jdbc = jdbc;
        this.maxLogBytes = maxLogBytes;
    }

    @PostMapping("/jobs")
    public Jobs.View submit(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody Jobs.Submit job,
            HttpServletRequest request) {
        Auth.User user = auth.requireUser(authorization, request);
        if (job.command() == null || job.command().isEmpty() || job.command().get(0).isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "command is required");
        }
        if (job.gpus() != null && (job.gpus() < 1 || job.gpus() > 8)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "gpus must be between 1 and 8");
        }
        if (job.vramMb() != null && job.vramMb() < 1) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "vramMb must be positive");
        }
        try {
            return jobs.find(jobs.submit(user.id(), job)).orElseThrow();
        } catch (DataIntegrityViolationException error) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "unknown node " + job.node());
        }
    }

    @GetMapping("/jobs")
    public List<Jobs.View> list(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestParam(defaultValue = "false") boolean all,
            HttpServletRequest request) {
        auth.requireUser(authorization, request);
        return jobs.list(all);
    }

    @GetMapping("/jobs/{id}")
    public Jobs.View get(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable long id,
            HttpServletRequest request) {
        auth.requireUser(authorization, request);
        return find(id);
    }

    @PostMapping("/jobs/{id}/cancel")
    public Jobs.View cancel(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable long id,
            HttpServletRequest request) {
        Auth.User user = auth.requireUser(authorization, request);
        if (!user.admin() && !find(id).user().equals(user.name())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "only the owner or an admin can cancel a job");
        }
        jobs.cancel(id);
        return find(id);
    }

    /** Output of the job's latest attempt from {@code offset}. The headers say where to continue and whether to. */
    @GetMapping("/jobs/{id}/logs")
    public ResponseEntity<byte[]> logs(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable long id,
            @RequestParam(defaultValue = "0") long offset,
            HttpServletRequest request) {
        auth.requireUser(authorization, request);
        Jobs.View job = find(id);
        byte[] data = job.attempt() == null ? new byte[0] : jobs.readLog(id, job.attempt(), offset);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header("X-Hyrax-Next-Offset", Long.toString(offset + data.length))
                .header("X-Hyrax-Job-State", job.state())
                .header("X-Hyrax-Attempt", job.attempt() == null ? "0" : job.attempt().toString())
                .body(data);
    }

    private Jobs.View find(long id) {
        return jobs.find(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "no job " + id));
    }

    // ---- agents ----

    /** Appends job output. The reply is the offset the next chunk must start at, so a repeat or a gap corrects itself. */
    @PostMapping(value = "/agent/jobs/{id}/attempts/{attempt}/logs", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Map<String, Long> appendLog(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable long id,
            @PathVariable int attempt,
            @RequestParam long offset,
            @RequestBody(required = false) byte[] data,
            HttpServletRequest request) {
        String node = auth.requireEnrolledNode(authorization, request);
        if (!jobs.attemptIsOn(id, attempt, node)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no such attempt on this node");
        }
        byte[] chunk = data == null ? new byte[0] : data;
        if (offset + chunk.length > maxLogBytes) {
            throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "log is full");
        }
        return Map.of("offset", jobs.appendLog(id, attempt, offset, chunk));
    }

    // ---- admins ----

    @PostMapping("/admin/users")
    public Map<String, String> addUser(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody NewUser user,
            HttpServletRequest request) {
        auth.requireAdmin(authorization, request);
        if (user.name() == null || user.name().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "name is required");
        }
        String token = Auth.newToken("hxu_");
        try {
            jdbc.update("insert into app_user (name, token_hash, is_admin) values (?, ?, ?)",
                    user.name(), Auth.hash(token), user.admin());
        } catch (DataIntegrityViolationException error) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "user " + user.name() + " already exists");
        }
        return Map.of("name", user.name(), "token", token);
    }

    /** Issues the node's own agent token, replacing any earlier one. From then on the shared token no longer works for it. */
    @PostMapping("/admin/nodes/{node}/enroll")
    public Map<String, String> enroll(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String node,
            HttpServletRequest request) {
        auth.requireAdmin(authorization, request);
        String token = Auth.newToken("hxn_");
        jdbc.update("""
                insert into node (name, last_seen, agent_token_hash) values (?, now(), ?)
                on conflict (name) do update set agent_token_hash = excluded.agent_token_hash
                """, node, Auth.hash(token));
        return Map.of("node", node, "token", token);
    }
}
