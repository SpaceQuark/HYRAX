package hyrax.server;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Job state in Postgres. Every transition is a conditional update on the current state, so the scheduler
 * and the heartbeat handlers can race and the first one wins.
 */
@Component
public class Jobs {

    /** States in which an attempt still holds its GPUs. LOST is one: the process may be running unseen. */
    private static final String LIVE = "('ASSIGNED', 'RUNNING', 'LOST')";

    private static final Set<String> FINAL_REPORTS = Set.of("SUCCEEDED", "FAILED", "CANCELLED", "REJECTED");

    public record Submit(
            String name, List<String> command, Map<String, String> env, Integer gpus, Long vramMb, String gpuModel, String node) {}

    public record View(
            long id,
            String user,
            String name,
            List<String> command,
            int gpus,
            Long vramMb,
            String gpuModel,
            String wantedNode,
            String state,
            Instant submittedAt,
            Instant finishedAt,
            boolean cancelRequested,
            Integer attempt,
            String node,
            List<Integer> gpuIndexes,
            Instant startedAt,
            Integer exitCode,
            String reason,
            @JsonRawValue String lastDecision) {}

    public record Queued(long id, int gpus, Long vramMb, String gpuModel, String node, List<String> requiredLabels) {}

    /** What the scheduler needs to know about GPUs and nodes beyond live telemetry. */
    public record Facts(Set<String> knownGpus, Set<String> reservedGpus, Set<String> allocatedGpus,
            Set<String> enrolledNodes, Map<String, Set<String>> labels) {}

    private record Live(long jobId, int attempt, String state, boolean cancelRequested, List<String> command,
            String env, List<String> gpus) {}

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final ObjectMapper json;
    private final long freeMaxComputeMb;

    public Jobs(JdbcTemplate jdbc, TransactionTemplate transaction, ObjectMapper json,
            @Value("${hyrax.jobs.free-max-compute-mb}") long freeMaxComputeMb) {
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.json = json;
        this.freeMaxComputeMb = freeMaxComputeMb;
    }

    // ---- submit, list, cancel ----

    public long submit(long userId, Submit job) {
        return jdbc.queryForObject("""
                insert into job (user_id, name, command, env, gpus, vram_mb, gpu_model, node)
                values (?, ?, array(select jsonb_array_elements_text(?::jsonb)), ?::jsonb, ?, ?, ?, ?)
                returning id
                """, Long.class,
                userId, job.name(), write(job.command()), write(job.env() == null ? Map.of() : job.env()),
                job.gpus() == null ? 1 : job.gpus(), job.vramMb(), job.gpuModel(), job.node());
    }

    private static final String VIEW = """
            select j.id, u.name as user_name, j.name, j.command, j.gpus, j.vram_mb, j.gpu_model, j.node as wanted_node,
                   j.state, j.submitted_at, j.finished_at, j.cancel_requested_at is not null as cancel_requested,
                   j.last_decision::text as last_decision,
                   a.attempt, a.node as run_node, a.started_at, a.exit_code, a.reason,
                   (select array_agg(g.index order by g.index)
                      from gpu_allocation ga join gpu g on g.uuid = ga.gpu_uuid
                     where ga.job_id = a.job_id and ga.attempt = a.attempt) as gpu_indexes
              from job j
              join app_user u on u.id = j.user_id
              left join lateral (
                   select * from job_attempt a where a.job_id = j.id order by a.attempt desc limit 1) a on true
            """;

    /** Unfinished jobs and those that finished in the last day; with {@code all}, the latest 200. */
    public List<View> list(boolean all) {
        String filter = all
                ? " order by j.id desc limit 200"
                : " where j.finished_at is null or j.finished_at > now() - interval '1 day' order by j.id desc limit 200";
        List<View> rows = new ArrayList<>(jdbc.query(VIEW + filter, Jobs::view));
        java.util.Collections.reverse(rows);
        return rows;
    }

    public Optional<View> find(long id) {
        return jdbc.query(VIEW + " where j.id = ?", Jobs::view, id).stream().findFirst();
    }

    /** A queued job is cancelled on the spot; a placed one once its agent has stopped the process. */
    public void cancel(long id) {
        int cancelled = jdbc.update(
                "update job set state = 'CANCELLED', cancel_requested_at = now(), finished_at = now() where id = ? and state = 'QUEUED'",
                id);
        if (cancelled == 0) {
            jdbc.update("""
                    update job set cancel_requested_at = coalesce(cancel_requested_at, now())
                     where id = ? and state in ('ASSIGNED', 'RUNNING', 'LOST')
                    """, id);
        }
    }

    // ---- scheduler ----

    public List<Queued> queued() {
        return jdbc.query("""
                select id, gpus, vram_mb, gpu_model, node, required_labels from job
                 where state = 'QUEUED' order by submitted_at, id
                """,
                (row, index) -> new Queued(row.getLong("id"), row.getInt("gpus"), (Long) row.getObject("vram_mb"),
                        row.getString("gpu_model"), row.getString("node"), strings(row.getArray("required_labels"))));
    }

    public Facts facts() {
        Set<String> known = new HashSet<>();
        Set<String> reserved = new HashSet<>();
        jdbc.query("select uuid, reserved from gpu", row -> {
            known.add(row.getString("uuid"));
            if (row.getBoolean("reserved")) {
                reserved.add(row.getString("uuid"));
            }
        });
        Map<String, Set<String>> labels = new HashMap<>();
        jdbc.query("select node, label from node_label",
                row -> { labels.computeIfAbsent(row.getString("node"), node -> new HashSet<>()).add(row.getString("label")); });
        return new Facts(known, reserved,
                new HashSet<>(jdbc.queryForList("select gpu_uuid from gpu_allocation where released_at is null", String.class)),
                new HashSet<>(jdbc.queryForList("select name from node where agent_token_hash is not null", String.class)),
                labels);
    }

    /**
     * Gives a queued job a new attempt on {@code node} holding {@code gpus}, all or nothing. Returns false when
     * the job is no longer queued or a GPU has been allocated in the meantime.
     */
    public boolean place(long jobId, String node, List<String> gpus, Duration lease, String decision) {
        try {
            return Boolean.TRUE.equals(transaction.execute(status -> {
                int claimed = jdbc.update("""
                        update job set state = 'ASSIGNED', last_decision = ?::jsonb, last_considered_at = now()
                         where id = ? and state = 'QUEUED' and cancel_requested_at is null
                        """, decision, jobId);
                if (claimed == 0) {
                    return false;
                }
                int attempt = jdbc.queryForObject(
                        "select coalesce(max(attempt), 0) + 1 from job_attempt where job_id = ?", Integer.class, jobId);
                jdbc.update("""
                        insert into job_attempt (job_id, attempt, node, lease_expires_at)
                        values (?, ?, ?, now() + ? * interval '1 millisecond')
                        """, jobId, attempt, node, lease.toMillis());
                for (String gpu : gpus) {
                    jdbc.update("insert into gpu_allocation (job_id, attempt, gpu_uuid) values (?, ?, ?)", jobId, attempt, gpu);
                }
                return true;
            }));
        } catch (DuplicateKeyException taken) {
            return false;
        }
    }

    public void recordDecision(long jobId, String decision) {
        jdbc.update("update job set last_decision = ?::jsonb, last_considered_at = now() where id = ? and state = 'QUEUED'",
                decision, jobId);
    }

    /** Requeues jobs whose agent never acknowledged the assignment. */
    public void expireLeases() {
        jdbc.query("select job_id, attempt from job_attempt where state = 'ASSIGNED' and lease_expires_at < now()",
                row -> { finish(row.getLong("job_id"), row.getInt("attempt"), "EXPIRED", null,
                        "the agent did not acknowledge the assignment in time"); });
    }

    public List<String> nodesWithRunningAttempts() {
        return jdbc.queryForList("select distinct node from job_attempt where state = 'RUNNING'", String.class);
    }

    /** The node has gone silent. Its jobs keep their GPUs and are not retried: they may still be running. */
    public void markLost(String node) {
        transaction.executeWithoutResult(status -> {
            List<Long> lost = jdbc.queryForList("""
                    update job_attempt set state = 'LOST', reason = 'the node stopped reporting'
                     where node = ? and state = 'RUNNING' returning job_id
                    """, Long.class, node);
            for (long jobId : lost) {
                jdbc.update("update job set state = 'LOST' where id = ?", jobId);
            }
        });
    }

    // ---- agent ----

    /**
     * One heartbeat's worth of reconciliation: records what the agent reports about its attempts and returns
     * what it should be running. Anything the agent runs that is not a live attempt of this node is stale and
     * gets cancelled, which is what fences an attempt that was expired or abandoned while the node was away.
     */
    public Heartbeat.Desired reconcile(String node, List<Heartbeat.Attempt> reported) {
        return transaction.execute(status -> {
            Map<Heartbeat.Ref, Live> live = new HashMap<>();
            for (Live attempt : liveOn(node)) {
                live.put(new Heartbeat.Ref(attempt.jobId(), attempt.attempt()), attempt);
            }
            List<Heartbeat.Ref> cancel = new ArrayList<>();
            List<Heartbeat.Ref> forget = new ArrayList<>();
            Set<Heartbeat.Ref> seen = new HashSet<>();

            for (Heartbeat.Attempt report : reported == null ? List.<Heartbeat.Attempt>of() : reported) {
                Heartbeat.Ref ref = new Heartbeat.Ref(report.jobId(), report.attempt());
                seen.add(ref);
                boolean finished = FINAL_REPORTS.contains(report.state());
                Live known = live.get(ref);
                if (known == null) {
                    (finished ? forget : cancel).add(ref);
                } else if (finished) {
                    finish(ref.jobId(), ref.attempt(), report.state(), report.exitCode(), report.reason());
                    live.remove(ref);
                    forget.add(ref);
                } else if (!"RUNNING".equals(known.state())) {
                    started(ref, report.pid());
                }
            }

            List<Heartbeat.Run> run = new ArrayList<>();
            for (Live attempt : List.copyOf(live.values())) {
                Heartbeat.Ref ref = new Heartbeat.Ref(attempt.jobId(), attempt.attempt());
                if (!seen.contains(ref) && !"ASSIGNED".equals(attempt.state())) {
                    // It was running here and the agent has no record of it: its state did not survive.
                    finish(ref.jobId(), ref.attempt(), "FAILED", null,
                            "the agent no longer knows this attempt; the machine probably rebooted");
                } else if (attempt.cancelRequested()) {
                    cancel.add(ref);
                } else {
                    run.add(new Heartbeat.Run(attempt.jobId(), attempt.attempt(), attempt.command(),
                            read(attempt.env()), attempt.gpus(), freeMaxComputeMb));
                }
            }
            return new Heartbeat.Desired(run, cancel, forget);
        });
    }

    private List<Live> liveOn(String node) {
        return jdbc.query("""
                select a.job_id, a.attempt, a.state, j.cancel_requested_at is not null as cancel_requested,
                       j.command, j.env::text as env,
                       array(select ga.gpu_uuid from gpu_allocation ga join gpu g on g.uuid = ga.gpu_uuid
                              where ga.job_id = a.job_id and ga.attempt = a.attempt order by g.index) as gpus
                  from job_attempt a join job j on j.id = a.job_id
                 where a.node = ? and a.state in %s
                """.formatted(LIVE),
                (row, index) -> new Live(row.getLong("job_id"), row.getInt("attempt"), row.getString("state"),
                        row.getBoolean("cancel_requested"), strings(row.getArray("command")), row.getString("env"),
                        strings(row.getArray("gpus"))),
                node);
    }

    private void started(Heartbeat.Ref ref, Integer pid) {
        int updated = jdbc.update("""
                update job_attempt set state = 'RUNNING', started_at = coalesce(started_at, now()), pid = ?, reason = null
                 where job_id = ? and attempt = ? and state in ('ASSIGNED', 'LOST')
                """, pid, ref.jobId(), ref.attempt());
        if (updated > 0) {
            jdbc.update("update job set state = 'RUNNING' where id = ?", ref.jobId());
        }
    }

    /**
     * Ends a live attempt and releases its GPUs. The job follows, except that an attempt which never ran
     * (EXPIRED, REJECTED), or failed with retries left, puts the job back in the queue.
     */
    public boolean finish(long jobId, int attempt, String state, Integer exitCode, String reason) {
        return Boolean.TRUE.equals(transaction.execute(status -> {
            int ended = jdbc.update("""
                    update job_attempt set state = ?, finished_at = now(), exit_code = ?, reason = ?
                     where job_id = ? and attempt = ? and state in %s
                    """.formatted(LIVE), state, exitCode, reason, jobId, attempt);
            if (ended == 0) {
                return false;
            }
            jdbc.update("update gpu_allocation set released_at = now() where job_id = ? and attempt = ? and released_at is null",
                    jobId, attempt);
            boolean neverRan = state.equals("EXPIRED") || state.equals("REJECTED");
            Map<String, Object> job = jdbc.queryForMap(
                    "select max_attempts, cancel_requested_at is not null as cancel_requested from job where id = ?", jobId);
            boolean cancelRequested = (Boolean) job.get("cancel_requested");
            boolean retry = neverRan || (state.equals("FAILED") && attempt < (Integer) job.get("max_attempts"));
            if (retry && !cancelRequested) {
                jdbc.update("update job set state = 'QUEUED' where id = ?", jobId);
            } else {
                jdbc.update("update job set state = ?, finished_at = now() where id = ?", neverRan ? "CANCELLED" : state, jobId);
            }
            return true;
        }));
    }

    // ---- logs ----

    public boolean attemptIsOn(long jobId, int attempt, String node) {
        return jdbc.queryForObject("select count(*) from job_attempt where job_id = ? and attempt = ? and node = ?",
                Integer.class, jobId, attempt, node) > 0;
    }

    public long logLength(long jobId, int attempt) {
        return jdbc.queryForObject("""
                select coalesce(max(byte_offset + octet_length(data)), 0) from job_log_chunk where job_id = ? and attempt = ?
                """, Long.class, jobId, attempt);
    }

    /** Appends a chunk if it starts exactly where the stored log ends. Returns the offset the next chunk must have. */
    public long appendLog(long jobId, int attempt, long offset, byte[] data) {
        long length = logLength(jobId, attempt);
        if (offset != length || data.length == 0) {
            return length;
        }
        jdbc.update("insert into job_log_chunk (job_id, attempt, byte_offset, data) values (?, ?, ?, ?) on conflict do nothing",
                jobId, attempt, offset, data);
        return logLength(jobId, attempt);
    }

    /** Up to about 1 MiB of log starting at {@code offset}. */
    public byte[] readLog(long jobId, int attempt, long offset) {
        var out = new java.io.ByteArrayOutputStream();
        jdbc.query("""
                select byte_offset, data from job_log_chunk
                 where job_id = ? and attempt = ? and byte_offset + octet_length(data) > ?
                 order by byte_offset
                """,
                row -> {
                    if (out.size() < 1024 * 1024) {
                        byte[] data = row.getBytes("data");
                        int skip = (int) Math.max(0, offset - row.getLong("byte_offset"));
                        out.write(data, skip, data.length - skip);
                    }
                },
                jobId, attempt, offset);
        return out.toByteArray();
    }

    // ---- mapping ----

    private static View view(ResultSet row, int index) throws SQLException {
        Array gpus = row.getArray("gpu_indexes");
        return new View(row.getLong("id"), row.getString("user_name"), row.getString("name"),
                strings(row.getArray("command")), row.getInt("gpus"), (Long) row.getObject("vram_mb"),
                row.getString("gpu_model"), row.getString("wanted_node"), row.getString("state"),
                instant(row, "submitted_at"), instant(row, "finished_at"), row.getBoolean("cancel_requested"),
                (Integer) row.getObject("attempt"), row.getString("run_node"),
                gpus == null ? List.of() : List.of((Integer[]) gpus.getArray()),
                instant(row, "started_at"), (Integer) row.getObject("exit_code"), row.getString("reason"),
                row.getString("last_decision"));
    }

    private static Instant instant(ResultSet row, String column) throws SQLException {
        OffsetDateTime value = row.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static List<String> strings(Array array) throws SQLException {
        return array == null ? List.of() : List.of((String[]) array.getArray());
    }

    String write(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException(error);
        }
    }

    private Map<String, String> read(String env) {
        try {
            return json.readValue(env, new TypeReference<Map<String, String>>() {});
        } catch (JsonProcessingException error) {
            throw new IllegalStateException(error);
        }
    }
}
