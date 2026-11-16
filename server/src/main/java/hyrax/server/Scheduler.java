package hyrax.server;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The scheduling loop. One thread, one pass every few seconds: expire unacknowledged assignments, mark jobs
 * on silent nodes as lost, then place queued jobs in submission order. A job that cannot be placed does not
 * block the ones behind it, and records why each GPU was turned down.
 */
@Component
public class Scheduler {

    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    private record Verdict(ClusterState.Node node, Heartbeat.Gpu gpu, String rejected) {}

    /** Per GPU, since when compute processes have held next to no memory. Absent while something is using it. */
    private final Map<String, Instant> freeSince = new ConcurrentHashMap<>();
    private final Instant startedAt = Instant.now();

    private final Jobs jobs;
    private final ClusterState cluster;
    private final Duration lease;
    private final Duration lostAfter;
    private final Duration freeFor;
    private final long freeMaxComputeMb;

    public Scheduler(
            Jobs jobs,
            ClusterState cluster,
            @Value("${hyrax.jobs.assign-lease}") Duration lease,
            @Value("${hyrax.jobs.lost-after}") Duration lostAfter,
            @Value("${hyrax.jobs.free-for}") Duration freeFor,
            @Value("${hyrax.jobs.free-max-compute-mb}") long freeMaxComputeMb) {
        this.jobs = jobs;
        this.cluster = cluster;
        this.lease = lease;
        this.lostAfter = lostAfter;
        this.freeFor = freeFor;
        this.freeMaxComputeMb = freeMaxComputeMb;
    }

    /** Called for every heartbeat, so that a GPU someone grabbed by hand stops looking free at once. */
    public void observe(Heartbeat heartbeat, Instant now) {
        for (Heartbeat.Gpu gpu : heartbeat.gpus()) {
            if (gpu.uuid() == null) {
                continue;
            }
            if (computeMemoryMb(gpu) <= freeMaxComputeMb) {
                freeSince.putIfAbsent(gpu.uuid(), now);
            } else {
                freeSince.remove(gpu.uuid());
            }
        }
    }

    private static long computeMemoryMb(Heartbeat.Gpu gpu) {
        return gpu.processes() == null ? 0 : gpu.processes().stream()
                .filter(process -> "compute".equals(process.type()))
                .map(Heartbeat.Process::memoryMb)
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .sum();
    }

    @Scheduled(fixedDelayString = "${hyrax.jobs.tick}", initialDelayString = "${hyrax.jobs.tick}")
    void tick() {
        try {
            pass(Instant.now());
        } catch (DataAccessException error) {
            log.warn("Scheduler pass failed, will retry: {}", error.getMessage());
        }
    }

    public void pass(Instant now) {
        jobs.expireLeases();
        List<ClusterState.Node> nodes = cluster.snapshot();
        markLost(nodes, now);

        List<Jobs.Queued> queue = jobs.queued();
        if (queue.isEmpty()) {
            return;
        }
        Jobs.Facts facts = jobs.facts();
        Set<String> taken = new HashSet<>(facts.allocatedGpus());
        for (Jobs.Queued job : queue) {
            place(job, nodes, facts, taken, now);
        }
    }

    private void markLost(List<ClusterState.Node> nodes, Instant now) {
        for (String name : jobs.nodesWithRunningAttempts()) {
            // After a restart nothing has reported yet, so silence is measured from startup at the earliest.
            Instant lastSeen = nodes.stream()
                    .filter(node -> node.node().equals(name))
                    .map(ClusterState.Node::lastSeen)
                    .filter(seen -> seen.isAfter(startedAt))
                    .findFirst()
                    .orElse(startedAt);
            if (Duration.between(lastSeen, now).compareTo(lostAfter) > 0) {
                log.warn("Node {} has been silent since {}; its running jobs are now LOST", name, lastSeen);
                jobs.markLost(name);
            }
        }
    }

    private void place(Jobs.Queued job, List<ClusterState.Node> nodes, Jobs.Facts facts, Set<String> taken, Instant now) {
        List<Verdict> verdicts = new ArrayList<>();
        for (ClusterState.Node node : nodes) {
            // A pinned job only cares about its node; listing every other GPU as "wrong node" would be noise.
            if (job.node() != null && !job.node().equals(node.node())) {
                continue;
            }
            String nodeProblem = nodeProblem(job, node, facts);
            for (Heartbeat.Gpu gpu : node.gpus()) {
                verdicts.add(new Verdict(node, gpu, nodeProblem != null ? nodeProblem : gpuProblem(job, gpu, facts, taken, now)));
            }
        }

        // Among nodes that can supply the GPUs: first the one with most of the labels the job prefers (its
        // datasets), then best fit, the node whose chosen GPUs are the smallest that satisfy the job, so big
        // cards stay free for jobs that need them. Preference never overrides eligibility.
        String chosenNode = null;
        List<Heartbeat.Gpu> chosen = List.of();
        long chosenSize = Long.MAX_VALUE;
        long chosenPreferred = -1;
        Map<String, List<Heartbeat.Gpu>> eligible = new LinkedHashMap<>();
        for (Verdict verdict : verdicts) {
            if (verdict.rejected() == null) {
                eligible.computeIfAbsent(verdict.node().node(), node -> new ArrayList<>()).add(verdict.gpu());
            }
        }
        for (var entry : eligible.entrySet()) {
            if (entry.getValue().size() < job.gpus()) {
                continue;
            }
            List<Heartbeat.Gpu> smallest = entry.getValue().stream()
                    .sorted(Comparator.comparingLong(Heartbeat.Gpu::memoryTotalMb).thenComparingInt(Heartbeat.Gpu::index))
                    .limit(job.gpus())
                    .toList();
            long size = smallest.get(smallest.size() - 1).memoryTotalMb();
            long preferred = preferredOn(job, entry.getKey(), facts).size();
            if (preferred > chosenPreferred || (preferred == chosenPreferred && size < chosenSize)) {
                chosenNode = entry.getKey();
                chosen = smallest;
                chosenSize = size;
                chosenPreferred = preferred;
            }
        }

        List<Map<String, Object>> decision = new ArrayList<>();
        if (verdicts.isEmpty()) {
            decision.add(Map.of("node", job.node() == null ? "*" : job.node(), "rejected", "no such node has reported"));
        }
        for (Verdict verdict : verdicts) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("node", verdict.node().node());
            entry.put("gpu", verdict.gpu().index());
            if (verdict.rejected() != null) {
                entry.put("rejected", verdict.rejected());
            } else if (chosen.contains(verdict.gpu()) && verdict.node().node().equals(chosenNode)) {
                entry.put("chosen", true);
                if (!job.preferredLabels().isEmpty()) {
                    List<String> has = preferredOn(job, chosenNode, facts);
                    entry.put("note", has.isEmpty()
                            ? "no node with a free GPU has " + String.join(", ", job.preferredLabels())
                            : "has " + String.join(", ", has));
                }
            } else if (eligible.get(verdict.node().node()).size() < job.gpus()) {
                entry.put("rejected", "node has only %d eligible GPU(s), job needs %d"
                        .formatted(eligible.get(verdict.node().node()).size(), job.gpus()));
            }
            decision.add(entry);
        }
        String decisionJson = jobs.write(decision);

        if (chosenNode == null) {
            jobs.recordDecision(job.id(), decisionJson);
            return;
        }
        List<String> uuids = chosen.stream().map(Heartbeat.Gpu::uuid).toList();
        if (jobs.place(job.id(), chosenNode, uuids, lease, decisionJson)) {
            taken.addAll(uuids);
            log.info("Job {} assigned to {} GPU {}", job.id(), chosenNode, chosen.stream().map(Heartbeat.Gpu::index).toList());
        }
    }

    private static List<String> preferredOn(Jobs.Queued job, String node, Jobs.Facts facts) {
        Set<String> labels = facts.labels().getOrDefault(node, Set.of());
        return job.preferredLabels().stream().filter(labels::contains).toList();
    }

    private static String nodeProblem(Jobs.Queued job, ClusterState.Node node, Jobs.Facts facts) {
        if (!node.online()) {
            return "node is offline";
        }
        if (!facts.enrolledNodes().contains(node.node())) {
            return "node's agent is not enrolled to run jobs";
        }
        Set<String> labels = facts.labels().getOrDefault(node.node(), Set.of());
        for (String label : job.requiredLabels()) {
            if (!labels.contains(label)) {
                return "node lacks label " + label;
            }
        }
        return null;
    }

    private String gpuProblem(Jobs.Queued job, Heartbeat.Gpu gpu, Jobs.Facts facts, Set<String> taken, Instant now) {
        if (gpu.uuid() == null || !facts.knownGpus().contains(gpu.uuid())) {
            return "GPU is not recorded yet";
        }
        if (facts.reservedGpus().contains(gpu.uuid())) {
            return "reserved";
        }
        if (taken.contains(gpu.uuid())) {
            return "allocated to another HYRAX job";
        }
        if (job.gpuModel() != null && !gpu.name().toLowerCase().contains(job.gpuModel().toLowerCase())) {
            return "model is %s, job asks for %s".formatted(gpu.name(), job.gpuModel());
        }
        if (job.vramMb() != null && gpu.memoryTotalMb() < job.vramMb()) {
            return "has %d MiB in total, job needs %d".formatted(gpu.memoryTotalMb(), job.vramMb());
        }
        long compute = computeMemoryMb(gpu);
        if (compute > freeMaxComputeMb) {
            return "an external process holds %d MiB".formatted(compute);
        }
        Instant since = freeSince.get(gpu.uuid());
        if (since == null || Duration.between(since, now).compareTo(freeFor) < 0) {
            return "free for only %ds, needs %ds".formatted(
                    since == null ? 0 : Duration.between(since, now).toSeconds(), freeFor.toSeconds());
        }
        long available = gpu.memoryTotalMb() - gpu.memoryUsedMb();
        if (job.vramMb() != null && available < job.vramMb()) {
            return "only %d MiB available, job needs %d".formatted(available, job.vramMb());
        }
        return null;
    }
}
