package hyrax.server;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Storage for idle episodes and process claims. */
@Component
public class Alerts {

    public record Alert(
            long id,
            String node,
            String gpuUuid,
            int gpuIndex,
            String gpuName,
            int pid,
            String osUser,
            String command,
            long memoryMb,
            Instant idleSince,
            String ownerSlackId,
            String slackChannel,
            String slackTs) {}

    private static final String COLUMNS = """
            id, node, gpu_uuid, gpu_index, gpu_name, pid, os_user, command, memory_mb,
            idle_since, owner_slack_id, slack_channel, slack_ts
            """;

    private final JdbcTemplate jdbc;

    public Alerts(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static Alert map(ResultSet row, int index) throws SQLException {
        return new Alert(
                row.getLong("id"),
                row.getString("node"),
                row.getString("gpu_uuid"),
                row.getInt("gpu_index"),
                row.getString("gpu_name"),
                row.getInt("pid"),
                row.getString("os_user"),
                row.getString("command"),
                row.getLong("memory_mb"),
                row.getObject("idle_since", OffsetDateTime.class).toInstant(),
                row.getString("owner_slack_id"),
                row.getString("slack_channel"),
                row.getString("slack_ts"));
    }

    private static OffsetDateTime utc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    /** Opens an episode, or returns empty if this process already has an open one. */
    public Optional<Alert> open(
            String node, Heartbeat.Gpu gpu, Heartbeat.Process process, Instant idleSince, Instant now) {
        return jdbc.query("""
                insert into alert (node, gpu_uuid, gpu_index, gpu_name, pid, os_user, command, memory_mb,
                    idle_since, opened_at, owner_slack_id)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    (select slack_user_id from process_owner where node = ? and pid = ?))
                on conflict (node, gpu_uuid, pid) where closed_at is null do nothing
                returning\s""" + COLUMNS,
                Alerts::map,
                node, gpu.uuid(), gpu.index(), gpu.name(), process.pid(), process.user(), process.command(),
                process.memoryMb(), utc(idleSince), utc(now), node, process.pid())
                .stream().findFirst();
    }

    public void close(long id, Instant now) {
        jdbc.update("update alert set closed_at = ? where id = ? and closed_at is null", utc(now), id);
    }

    public Optional<Alert> find(long id) {
        return jdbc.query("select " + COLUMNS + " from alert where id = ?", Alerts::map, id).stream().findFirst();
    }

    public List<Alert> findOpen() {
        return jdbc.query("select " + COLUMNS + " from alert where closed_at is null", Alerts::map);
    }

    public void recordMessage(long id, String channel, String ts) {
        jdbc.update("update alert set slack_channel = ?, slack_ts = ? where id = ?", channel, ts, id);
    }

    /** Binds the alert's process to a Slack user, so this and later alerts for it go to them. */
    public void claim(Alert alert, String slackUserId) {
        jdbc.update("""
                insert into process_owner (node, pid, slack_user_id) values (?, ?, ?)
                on conflict (node, pid) do update set slack_user_id = excluded.slack_user_id, claimed_at = now()
                """,
                alert.node(), alert.pid(), slackUserId);
        jdbc.update("update alert set owner_slack_id = ? where id = ?", slackUserId, alert.id());
    }

    public void forgetOwner(String node, int pid) {
        jdbc.update("delete from process_owner where node = ? and pid = ?", node, pid);
    }

    public void remindAt(long id, Instant when) {
        jdbc.update("update alert set remind_at = ? where id = ?", utc(when), id);
    }

    /** Open episodes whose snooze has run out. Each is returned once. */
    public List<Alert> takeDueReminders(Instant now) {
        return jdbc.query("""
                update alert set remind_at = null
                where closed_at is null and remind_at <= ?
                returning\s""" + COLUMNS,
                Alerts::map, utc(now));
    }
}
