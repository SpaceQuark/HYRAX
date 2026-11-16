package hyrax.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
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

    public record NewLabel(String label, String path) {}

    public record Artifacts(long jobId, Integer attempt, List<Jobs.Artifact> files) {}

    private final Auth auth;
    private final Jobs jobs;
    private final Storage storage;
    private final JdbcTemplate jdbc;
    private final long maxLogBytes;
    private final long maxSnapshotBytes;
    private final long maxArtifactBytes;

    public JobController(
            Auth auth,
            Jobs jobs,
            Storage storage,
            JdbcTemplate jdbc,
            @Value("${hyrax.jobs.max-log-bytes}") long maxLogBytes,
            @Value("${hyrax.snapshots.max-bytes}") long maxSnapshotBytes,
            @Value("${hyrax.artifacts.max-bytes}") long maxArtifactBytes) {
        this.auth = auth;
        this.jobs = jobs;
        this.storage = storage;
        this.jdbc = jdbc;
        this.maxLogBytes = maxLogBytes;
        this.maxSnapshotBytes = maxSnapshotBytes;
        this.maxArtifactBytes = maxArtifactBytes;
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
        if (job.environment() != null && !Set.of("none", "uv").contains(job.environment())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "environment must be none or uv");
        }
        if ("uv".equals(job.environment()) && job.snapshotId() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "a uv environment needs a source snapshot");
        }
        String workdir = job.workdir() == null ? "." : job.workdir();
        if (workdir.startsWith("/") || List.of(workdir.split("/")).contains("..")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "workdir must be a path inside the snapshot");
        }
        try {
            return jobs.find(jobs.submit(user.id(), job)).orElseThrow();
        } catch (DataIntegrityViolationException error) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "unknown node or snapshot");
        }
    }

    // ---- source snapshots ----

    /** Lets the CLI skip the upload when the server already has this exact tree. */
    @GetMapping("/snapshots/{sha256}")
    public Map<String, Long> findSnapshot(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String sha256,
            HttpServletRequest request) {
        auth.requireUser(authorization, request);
        Long id = jobs.snapshotId(sha256)
                .filter(found -> Files.exists(storage.snapshot(sha256)))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "no such snapshot"));
        return Map.of("id", id);
    }

    @PutMapping("/snapshots/{sha256}")
    public Map<String, Long> uploadSnapshot(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String sha256,
            @RequestParam(required = false) String gitCommit,
            @RequestParam(defaultValue = "false") boolean gitDirty,
            HttpServletRequest request) throws IOException {
        Auth.User user = auth.requireUser(authorization, request);
        Storage.Stored stored = storage.write(request.getInputStream(), storage.snapshot(sha256), maxSnapshotBytes, sha256);
        return Map.of("id", jobs.recordSnapshot(sha256, stored.bytes(), gitCommit, gitDirty, user.id()));
    }

    /** For agents: the snapshot of a job this node is running, and no other. */
    @GetMapping("/agent/snapshots/{id}")
    public ResponseEntity<Resource> downloadSnapshot(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable long id,
            HttpServletRequest request) {
        String node = auth.requireEnrolledNode(authorization, request);
        String sha256 = jobs.snapshotFor(id, node)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "no such snapshot for this node"));
        return file(storage.snapshot(sha256));
    }

    // ---- job outputs ----

    /** One file from the attempt's output directory. Uploading it again replaces it. */
    @PutMapping("/agent/jobs/{id}/attempts/{attempt}/artifacts")
    public Map<String, Long> uploadArtifact(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable long id,
            @PathVariable int attempt,
            @RequestParam String path,
            @RequestParam String sha256,
            HttpServletRequest request) throws IOException {
        String node = auth.requireEnrolledNode(authorization, request);
        if (!jobs.attemptIsOn(id, attempt, node)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no such attempt on this node");
        }
        long room = maxArtifactBytes - jobs.artifactBytes(id, attempt, path);
        Storage.Stored stored = storage.write(request.getInputStream(), storage.artifact(id, attempt, path), room, sha256);
        jobs.recordArtifact(id, attempt, path, stored.bytes(), stored.sha256());
        return Map.of("bytes", stored.bytes());
    }

    /** The manifest of the job's latest attempt: every output file with its size and hash. */
    @GetMapping("/jobs/{id}/artifacts")
    public Artifacts artifacts(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable long id,
            HttpServletRequest request) {
        auth.requireUser(authorization, request);
        Jobs.View job = find(id);
        return new Artifacts(id, job.attempt(), job.attempt() == null ? List.of() : jobs.artifacts(id, job.attempt()));
    }

    @GetMapping("/jobs/{id}/artifacts/file")
    public ResponseEntity<Resource> artifact(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable long id,
            @RequestParam String path,
            HttpServletRequest request) {
        auth.requireUser(authorization, request);
        Jobs.View job = find(id);
        if (job.attempt() == null || jobs.artifacts(id, job.attempt()).stream().noneMatch(file -> file.path().equals(path))) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no such output");
        }
        return file(storage.artifact(id, job.attempt(), path));
    }

    private static ResponseEntity<Resource> file(Path path) {
        if (!Files.isRegularFile(path)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "the file is no longer stored");
        }
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM).body(new FileSystemResource(path));
    }

    // ---- labels ----

    @GetMapping("/labels")
    public List<Jobs.Label> labels(
            @RequestHeader(value = "Authorization", required = false) String authorization, HttpServletRequest request) {
        auth.requireUser(authorization, request);
        return jobs.labels();
    }

    /** Says what a node has locally, e.g. label {@code dataset:camelyon16} at path {@code /data/camelyon16}. */
    @PutMapping("/admin/nodes/{node}/labels")
    public Jobs.Label setLabel(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String node,
            @RequestBody NewLabel label,
            HttpServletRequest request) {
        auth.requireAdmin(authorization, request);
        if (label.label() == null || !label.label().matches("[a-z]+:[A-Za-z0-9._-]+")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "a label looks like dataset:name");
        }
        if (label.path() != null && !label.path().startsWith("/")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "the path must be absolute");
        }
        try {
            jobs.setLabel(node, label.label(), label.path());
        } catch (DataIntegrityViolationException error) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "unknown node " + node);
        }
        return new Jobs.Label(node, label.label(), label.path());
    }

    @DeleteMapping("/admin/nodes/{node}/labels")
    public void removeLabel(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String node,
            @RequestParam String label,
            HttpServletRequest request) {
        auth.requireAdmin(authorization, request);
        jobs.removeLabel(node, label);
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
