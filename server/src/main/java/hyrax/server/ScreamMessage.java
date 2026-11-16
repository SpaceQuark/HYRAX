package hyrax.server;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The Slack message for an idle episode, as Block Kit structures ready to serialise.
 */
final class ScreamMessage {

    static final String CLAIM_ACTION = "scream_claim";
    static final String SNOOZE_ACTION = "scream_snooze";

    private ScreamMessage() {}

    static String fallbackText(Alerts.Alert alert, Instant now) {
        return "Idle GPU memory on %s, GPU %d: PID %d has held %s for %s."
                .formatted(alert.node(), alert.gpuIndex(), alert.pid(), memory(alert), idleFor(alert, now));
    }

    /** The alert with its buttons. "This is mine" is offered only while nobody owns the process. */
    static List<Map<String, Object>> withButtons(Alerts.Alert alert, Instant now, Duration snooze) {
        List<Map<String, Object>> buttons = new ArrayList<>();
        String value = Long.toString(alert.id());
        if (alert.ownerSlackId() == null) {
            buttons.add(Map.of(
                    "type", "button",
                    "action_id", CLAIM_ACTION,
                    "style", "primary",
                    "text", Map.of("type", "plain_text", "text", "This is mine"),
                    "value", value));
        }
        buttons.add(Map.of(
                "type", "button",
                "action_id", SNOOZE_ACTION,
                "text", Map.of("type", "plain_text", "text", "Snooze " + words(snooze)),
                "value", value));
        List<Map<String, Object>> blocks = summary(alert, now);
        blocks.add(Map.of("type", "actions", "block_id", "scream_actions", "elements", buttons));
        return blocks;
    }

    /** The alert after a button was used: the buttons give way to a line saying what happened. */
    static List<Map<String, Object>> resolved(Alerts.Alert alert, Instant now, String outcome) {
        List<Map<String, Object>> blocks = summary(alert, now);
        blocks.add(context(outcome));
        return blocks;
    }

    private static List<Map<String, Object>> summary(Alerts.Alert alert, Instant now) {
        String text = "*Idle GPU memory on %s, GPU %d*\nPID %d has held %s for %s at near 0%% utilization.\n`%s`"
                .formatted(alert.node(), alert.gpuIndex(), alert.pid(), memory(alert), idleFor(alert, now),
                        command(alert));
        String details = "%s · OS user `%s` · HYRAX only pings, it never kills."
                .formatted(alert.gpuName().replaceFirst("^NVIDIA (GeForce )?", ""),
                        alert.osUser() == null ? "?" : alert.osUser());
        List<Map<String, Object>> blocks = new ArrayList<>();
        blocks.add(Map.of("type", "section", "text", Map.of("type", "mrkdwn", "text", text)));
        blocks.add(context(details));
        return blocks;
    }

    private static Map<String, Object> context(String text) {
        return Map.of("type", "context", "elements", List.of(Map.of("type", "mrkdwn", "text", text)));
    }

    private static String command(Alerts.Alert alert) {
        if (alert.command() == null) {
            return "?";
        }
        // Backticks would end the code span; long commands would bury the message.
        String command = alert.command().replace('`', '\'');
        return command.length() <= 200 ? command : command.substring(0, 200) + "…";
    }

    private static String memory(Alerts.Alert alert) {
        return String.format(Locale.ROOT, "%.1f GiB", alert.memoryMb() / 1024.0);
    }

    private static String idleFor(Alerts.Alert alert, Instant now) {
        return words(Duration.between(alert.idleSince(), now));
    }

    static String words(Duration duration) {
        long hours = duration.toHours();
        int minutes = duration.toMinutesPart();
        if (hours > 0 && minutes > 0) {
            return hours + " h " + minutes + " min";
        }
        if (hours > 0) {
            return hours == 1 ? "1 hour" : hours + " hours";
        }
        return minutes + " min";
    }
}
