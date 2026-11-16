package hyrax.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

/** Placement and the attempt state machine, driven by hand: the scheduler's own timer is parked. */
@SpringBootTest(properties = {"hyrax.jobs.tick=1h", "hyrax.jobs.free-for=0s", "hyrax.jobs.assign-lease=30s",
        "hyrax.data-dir=${java.io.tmpdir}/hyrax-test-data", "hyrax.artifacts.max-bytes=64"})
@AutoConfigureMockMvc
@Import(TestDatabase.class)
class JobLifecycleTest {

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    Jobs jobs;

    @Autowired
    Scheduler scheduler;

    @Autowired
    ClusterState cluster;

    @Autowired
    MockMvc mvc;

    long user;

    @BeforeEach
    void seed() {
        jdbc.update("delete from job_log_chunk");
        jdbc.update("delete from job_artifact");
        jdbc.update("delete from node_label");
        jdbc.update("delete from gpu_allocation");
        jdbc.update("delete from job_attempt");
        jdbc.update("delete from job");
        jdbc.update("delete from source_snapshot");
        jdbc.update("delete from app_user");
        user = jdbc.queryForObject("insert into app_user (name, token_hash) values ('ada', ?) returning id",
                Long.class, Auth.hash("ada-token"));
        // small: two 24 GB cards. big: one 96 GB card. private: reserved. guest: reports but is not enrolled.
        report("small", gpu(0, "GPU-s0", "RTX 4090", 24564, 0), gpu(1, "GPU-s1", "RTX 4090", 24564, 0));
        report("big", gpu(0, "GPU-b0", "RTX PRO 6000", 97887, 0));
        report("private", gpu(0, "GPU-p0", "RTX 4090", 24564, 0));
        report("guest", gpu(0, "GPU-g0", "RTX 4090", 24564, 0));
        jdbc.update("update gpu set reserved = true where uuid = 'GPU-p0'");
        jdbc.update("update node set agent_token_hash = name || '-hash' where name <> 'guest'");
    }

    private static Heartbeat.Gpu gpu(int index, String uuid, String name, long totalMb, long computeMb) {
        var processes = computeMb == 0
                ? List.<Heartbeat.Process>of()
                : List.of(new Heartbeat.Process(7, "compute", "alice", "python by_hand.py", computeMb));
        return new Heartbeat.Gpu(index, uuid, name, 0, computeMb, totalMb, 40, 30.0, processes);
    }

    private void report(String node, Heartbeat.Gpu... gpus) {
        var heartbeat = new Heartbeat(node, "0.2.0", List.of(gpus), List.of());
        jdbc.update("insert into node (name, last_seen) values (?, now()) on conflict do nothing", node);
        for (Heartbeat.Gpu gpu : gpus) {
            jdbc.update("""
                    insert into gpu (uuid, node, index, name, memory_total_mb) values (?, ?, ?, ?, ?)
                    on conflict (uuid) do update set node = excluded.node, reserved = false
                    """, gpu.uuid(), node, gpu.index(), gpu.name(), gpu.memoryTotalMb());
        }
        cluster.record(heartbeat);
        scheduler.observe(heartbeat, Instant.now().minusSeconds(1));
    }

    private long submit(Long vramMb, String node) {
        return jobs.submit(user, new Jobs.Submit(null, List.of("python", "train.py"), Map.of("SEED", "1"), 1, vramMb, null, node,
                null, null, null, null, null));
    }

    private Jobs.View job(long id) {
        return jobs.find(id).orElseThrow();
    }

    private static Heartbeat.Attempt attempt(long job, int attempt, String state, Integer exitCode) {
        return new Heartbeat.Attempt(job, attempt, state, 4242, exitCode, null);
    }

    @Test
    void roamsToTheSmallestGpuThatFits() {
        long small = submit(20_000L, null);
        long large = submit(40_000L, null);

        scheduler.pass(Instant.now());

        assertThat(job(small).node()).isEqualTo("small");
        assertThat(job(large).node()).isEqualTo("big");
        assertThat(job(large).lastDecision())
                .contains("has 24564 MiB in total, job needs 40000")
                .contains("reserved")
                .contains("not enrolled");
    }

    @Test
    void neverPlacesOnAGpuSomeoneIsUsingByHandAndSaysWhy() {
        report("small", gpu(0, "GPU-s0", "RTX 4090", 24564, 9000), gpu(1, "GPU-s1", "RTX 4090", 24564, 9000));
        long pinned = submit(null, "small");

        scheduler.pass(Instant.now());

        assertThat(job(pinned).state()).isEqualTo("QUEUED");
        assertThat(job(pinned).lastDecision()).contains("an external process holds 9000 MiB");
    }

    @Test
    void concurrentJobsNeverShareAGpu() {
        long first = submit(null, "big");
        long second = submit(null, "big");

        scheduler.pass(Instant.now());

        assertThat(job(first).state()).isEqualTo("ASSIGNED");
        assertThat(job(second).state()).isEqualTo("QUEUED");
        assertThat(job(second).lastDecision()).contains("allocated to another HYRAX job");
        // Even if the scheduler tried, the database refuses.
        assertThat(jobs.place(second, "big", List.of("GPU-b0"), java.time.Duration.ofSeconds(30), "[]")).isFalse();
        assertThat(job(second).state()).isEqualTo("QUEUED");
    }

    @Test
    void runsToCompletionAndReleasesTheGpu() {
        long id = submit(null, "big");
        scheduler.pass(Instant.now());

        Heartbeat.Desired desired = jobs.reconcile("big", List.of());
        assertThat(desired.run()).singleElement().satisfies(run -> {
            assertThat(run.jobId()).isEqualTo(id);
            assertThat(run.attempt()).isEqualTo(1);
            assertThat(run.command()).containsExactly("python", "train.py");
            assertThat(run.env()).containsEntry("SEED", "1");
            assertThat(run.gpus()).containsExactly("GPU-b0");
        });
        // Other nodes are told nothing about it.
        assertThat(jobs.reconcile("small", List.of()).run()).isEmpty();

        jobs.reconcile("big", List.of(attempt(id, 1, "RUNNING", null)));
        assertThat(job(id).state()).isEqualTo("RUNNING");

        desired = jobs.reconcile("big", List.of(attempt(id, 1, "SUCCEEDED", 0)));
        assertThat(desired.forget()).containsExactly(new Heartbeat.Ref(id, 1));
        assertThat(job(id).state()).isEqualTo("SUCCEEDED");
        assertThat(job(id).exitCode()).isZero();
        assertThat(jdbc.queryForObject("select count(*) from gpu_allocation where released_at is null", Integer.class)).isZero();
    }

    @Test
    void anUnacknowledgedAssignmentIsRequeuedAndTheLateAttemptIsFenced() {
        long id = submit(null, "big");
        scheduler.pass(Instant.now());
        jdbc.update("update job_attempt set lease_expires_at = now() - interval '1 second'");

        jobs.expireLeases();
        assertThat(job(id).state()).isEqualTo("QUEUED");

        // The agent started attempt 1 after all. It is told to stop it, never to keep it.
        Heartbeat.Desired desired = jobs.reconcile("big", List.of(attempt(id, 1, "RUNNING", null)));
        assertThat(desired.cancel()).containsExactly(new Heartbeat.Ref(id, 1));
        assertThat(desired.run()).isEmpty();

        // While it is still there the GPU is not free again in the scheduler's eyes until released: attempt 2 follows.
        scheduler.pass(Instant.now());
        assertThat(job(id).attempt()).isEqualTo(2);
    }

    @Test
    void cancelStopsARunningJobThroughTheAgent() {
        long id = submit(null, "big");
        scheduler.pass(Instant.now());
        jobs.reconcile("big", List.of(attempt(id, 1, "RUNNING", null)));

        jobs.cancel(id);
        Heartbeat.Desired desired = jobs.reconcile("big", List.of(attempt(id, 1, "RUNNING", null)));
        assertThat(desired.cancel()).containsExactly(new Heartbeat.Ref(id, 1));
        assertThat(job(id).state()).isEqualTo("RUNNING");

        jobs.reconcile("big", List.of(attempt(id, 1, "CANCELLED", -15)));
        assertThat(job(id).state()).isEqualTo("CANCELLED");
    }

    @Test
    void aRunningAttemptTheAgentForgotIsFailedNotLeftRunning() {
        long id = submit(null, "big");
        scheduler.pass(Instant.now());
        jobs.reconcile("big", List.of(attempt(id, 1, "RUNNING", null)));

        jobs.reconcile("big", List.of());

        assertThat(job(id).state()).isEqualTo("FAILED");
        assertThat(job(id).reason()).contains("rebooted");
    }

    @Test
    void aLostJobKeepsItsGpuAndResumesWhenTheNodeReturns() {
        long id = submit(null, "big");
        scheduler.pass(Instant.now());
        jobs.reconcile("big", List.of(attempt(id, 1, "RUNNING", null)));

        jobs.markLost("big");
        assertThat(job(id).state()).isEqualTo("LOST");
        long waiting = submit(null, "big");
        scheduler.pass(Instant.now());
        assertThat(job(waiting).state()).isEqualTo("QUEUED");

        jobs.reconcile("big", List.of(attempt(id, 1, "RUNNING", null)));
        assertThat(job(id).state()).isEqualTo("RUNNING");
    }

    @Test
    void logsAppendOnceAndInOrder() {
        long id = submit(null, "big");
        scheduler.pass(Instant.now());

        assertThat(jobs.appendLog(id, 1, 0, "hello ".getBytes())).isEqualTo(6);
        assertThat(jobs.appendLog(id, 1, 0, "hello ".getBytes())).isEqualTo(6);
        assertThat(jobs.appendLog(id, 1, 9, "gap".getBytes())).isEqualTo(6);
        assertThat(jobs.appendLog(id, 1, 6, "world".getBytes())).isEqualTo(11);
        assertThat(new String(jobs.readLog(id, 1, 3))).isEqualTo("lo world");
    }

    @Test
    void jobsAreRefusedOverPlainHttpAndWithoutAToken() throws Exception {
        String body = "{\"command\": [\"nvidia-smi\"]}";
        mvc.perform(post("/api/jobs").header("Authorization", "Bearer ada-token")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isForbidden());
        mvc.perform(post("/api/jobs").secure(true).contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/api/jobs").secure(true).header("Authorization", "Bearer ada-token")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("QUEUED"))
                .andExpect(jsonPath("$.user").value("ada"));
    }

    @Test
    void anEnrolledNodeGetsWorkOnlyWithItsOwnTokenOverHttps() throws Exception {
        jdbc.update("update node set agent_token_hash = ? where name = 'big'", Auth.hash("big-token"));
        long id = submit(null, "big");
        scheduler.pass(Instant.now());
        String heartbeat = """
                {"node": "big", "agentVersion": "0.2.0", "attempts": [],
                 "gpus": [{"index": 0, "uuid": "GPU-b0", "name": "RTX PRO 6000", "utilization": 0,
                           "memoryUsedMb": 0, "memoryTotalMb": 97887, "processes": []}]}
                """;

        mvc.perform(post("/api/agent/heartbeat").secure(true).header("Authorization", "Bearer wrong")
                        .contentType(MediaType.APPLICATION_JSON).content(heartbeat))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/api/agent/heartbeat").header("Authorization", "Bearer big-token")
                        .contentType(MediaType.APPLICATION_JSON).content(heartbeat))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.run").isEmpty());
        mvc.perform(post("/api/agent/heartbeat").secure(true).header("Authorization", "Bearer big-token")
                        .contentType(MediaType.APPLICATION_JSON).content(heartbeat))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.run[0].jobId").value(id));
    }

    private static String sha256(byte[] data) throws Exception {
        return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(data));
    }

    @Test
    void prefersTheNodeThatHasTheDatasetButNotAtTheCostOfVram() {
        jobs.setLabel("big", "dataset:slides", "/data/slides");
        long fits = jobs.submit(user, new Jobs.Submit(null, List.of("python", "train.py"), null, 1, 20_000L, null, null,
                null, null, null, null, List.of("dataset:slides")));
        scheduler.pass(Instant.now());
        // Best fit alone would have picked a 24 GB card on "small".
        assertThat(job(fits).node()).isEqualTo("big");
        assertThat(job(fits).lastDecision()).contains("has dataset:slides");
        assertThat(jobs.reconcile("big", List.of()).run().get(0).datasets()).containsEntry("slides", "/data/slides");

        // The only node with the dataset is now busy: the job still runs, elsewhere, and is told so.
        long elsewhere = jobs.submit(user, new Jobs.Submit(null, List.of("python", "train.py"), null, 1, 20_000L, null, null,
                null, null, null, null, List.of("dataset:slides")));
        scheduler.pass(Instant.now());
        assertThat(job(elsewhere).node()).isEqualTo("small");
        assertThat(job(elsewhere).lastDecision()).contains("no node with a free GPU has dataset:slides");
        assertThat(jobs.reconcile("small", List.of()).run().get(0).datasets()).isEmpty();

        // Required is different: it waits.
        long strict = jobs.submit(user, new Jobs.Submit(null, List.of("python", "train.py"), null, 1, null, null, null,
                null, null, null, List.of("dataset:slides"), null));
        scheduler.pass(Instant.now());
        assertThat(job(strict).state()).isEqualTo("QUEUED");
        assertThat(job(strict).lastDecision()).contains("node lacks label dataset:slides");
    }

    @Test
    void aSnapshotIsStoredOnceCheckedAndOnlyGivenToTheNodeRunningIt() throws Exception {
        byte[] tree = "pretend this is a tarball".getBytes();
        String sha = sha256(tree);
        jdbc.update("update node set agent_token_hash = ? where name = 'big'", Auth.hash("big-token"));
        jdbc.update("update node set agent_token_hash = ? where name = 'small'", Auth.hash("small-token"));

        mvc.perform(put("/api/snapshots/" + sha256("other".getBytes())).secure(true)
                        .header("Authorization", "Bearer ada-token").content(tree))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/snapshots/" + sha).secure(true).header("Authorization", "Bearer ada-token"))
                .andExpect(status().isNotFound());
        mvc.perform(put("/api/snapshots/" + sha + "?gitCommit=abc123&gitDirty=true").secure(true)
                        .header("Authorization", "Bearer ada-token").content(tree))
                .andExpect(status().isOk());
        long snapshot = jdbc.queryForObject("select id from source_snapshot where sha256 = ?", Long.class, sha);

        long id = jobs.submit(user, new Jobs.Submit(null, List.of("python", "train.py"), null, 1, null, null, "big",
                snapshot, "experiments", "uv", null, null));
        scheduler.pass(Instant.now());
        Heartbeat.Run run = jobs.reconcile("big", List.of()).run().get(0);
        assertThat(run.snapshot()).isEqualTo(new Heartbeat.Snapshot(snapshot, sha));
        assertThat(run.workdir()).isEqualTo("experiments");
        assertThat(run.environment()).isEqualTo("uv");
        assertThat(job(id).gitCommit()).isEqualTo("abc123");
        assertThat(job(id).gitDirty()).isTrue();

        mvc.perform(get("/api/agent/snapshots/" + snapshot).secure(true).header("Authorization", "Bearer big-token"))
                .andExpect(status().isOk())
                .andExpect(content().bytes(tree));
        mvc.perform(get("/api/agent/snapshots/" + snapshot).secure(true).header("Authorization", "Bearer small-token"))
                .andExpect(status().isNotFound());
    }

    @Test
    void outputsAreStoredWithChecksumsWithinALimitAndNeverOutsideTheirDirectory() throws Exception {
        jdbc.update("update node set agent_token_hash = ? where name = 'big'", Auth.hash("big-token"));
        long id = submit(null, "big");
        scheduler.pass(Instant.now());
        byte[] metrics = "{\"loss\": 0.1}".getBytes();
        String base = "/api/agent/jobs/" + id + "/attempts/1/artifacts";

        mvc.perform(put(base + "?path=run/metrics.json&sha256=" + sha256(metrics)).secure(true)
                        .header("Authorization", "Bearer big-token").content(metrics))
                .andExpect(status().isOk());
        mvc.perform(put(base + "?path=../../escape&sha256=" + sha256(metrics)).secure(true)
                        .header("Authorization", "Bearer big-token").content(metrics))
                .andExpect(status().isBadRequest());
        mvc.perform(put(base + "?path=/etc/passwd&sha256=" + sha256(metrics)).secure(true)
                        .header("Authorization", "Bearer big-token").content(metrics))
                .andExpect(status().isBadRequest());
        mvc.perform(put(base + "?path=corrupt&sha256=" + sha256("something else".getBytes())).secure(true)
                        .header("Authorization", "Bearer big-token").content(metrics))
                .andExpect(status().isBadRequest());
        // The test limit is 64 bytes per attempt.
        mvc.perform(put(base + "?path=huge.bin&sha256=" + sha256(new byte[100])).secure(true)
                        .header("Authorization", "Bearer big-token").content(new byte[100]))
                .andExpect(status().isPayloadTooLarge());

        mvc.perform(get("/api/jobs/" + id + "/artifacts").secure(true).header("Authorization", "Bearer ada-token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.files.length()").value(1))
                .andExpect(jsonPath("$.files[0].path").value("run/metrics.json"))
                .andExpect(jsonPath("$.files[0].sha256").value(sha256(metrics)));
        mvc.perform(get("/api/jobs/" + id + "/artifacts/file?path=run/metrics.json").secure(true)
                        .header("Authorization", "Bearer ada-token"))
                .andExpect(status().isOk())
                .andExpect(content().bytes(metrics));
        mvc.perform(get("/api/jobs/" + id + "/artifacts/file?path=../../../etc/passwd").secure(true)
                        .header("Authorization", "Bearer ada-token"))
                .andExpect(status().isNotFound());
    }
}
