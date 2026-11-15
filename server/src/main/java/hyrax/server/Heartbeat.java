package hyrax.server;

import java.util.List;
import java.util.Map;

/** What an agent sends every few seconds. */
public record Heartbeat(String node, String agentVersion, List<Gpu> gpus, List<Attempt> attempts) {

    public record Gpu(
            int index,
            String uuid,
            String name,
            Integer utilization,
            long memoryUsedMb,
            long memoryTotalMb,
            Integer temperatureC,
            Double powerW,
            List<Process> processes) {}

    public record Process(int pid, String type, String user, String command, Long memoryMb) {}

    /**
     * A job attempt as the agent sees it, keyed by (jobId, attempt). State is RUNNING, SUCCEEDED, FAILED,
     * CANCELLED or REJECTED (the agent refused to start it).
     */
    public record Attempt(long jobId, int attempt, String state, Integer pid, Integer exitCode, String reason) {}

    public record Ref(long jobId, int attempt) {}

    /** An attempt the node should be running. Starting it twice is harmless: the agent keys on (jobId, attempt). */
    public record Run(
            long jobId, int attempt, List<String> command, Map<String, String> env, List<String> gpus, long freeMaxComputeMb) {}

    /**
     * The reply. {@code run}: attempts this node should be running. {@code cancel}: attempts to stop, including
     * stale ones the server no longer recognises. {@code forget}: finished attempts the server has recorded.
     */
    public record Desired(List<Run> run, List<Ref> cancel, List<Ref> forget) {
        public static Desired nothing() {
            return new Desired(List.of(), List.of(), List.of());
        }
    }
}
