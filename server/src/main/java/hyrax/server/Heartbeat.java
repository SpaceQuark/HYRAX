package hyrax.server;

import java.util.List;

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

    /** A job attempt, keyed by (jobId, attempt). Unused until milestone 3. */
    public record Attempt(long jobId, int attempt, String state) {}

    /** The reply: the attempts this node should be running, and any to cancel. */
    public record Desired(List<Attempt> run, List<Attempt> cancel) {}
}
