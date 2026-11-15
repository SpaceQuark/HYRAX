package hyrax.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest(properties = {"hyrax.idle.alert-after=2h", "hyrax.idle.basking-after=10m"})
@Import(TestDatabase.class)
class IdleTrackerTest {

    private static final Instant START = Instant.parse("2026-11-14T10:00:00Z");
    private static final String NODE = "node-idle";
    private static final String GPU = "GPU-idle";

    @Autowired
    IdleTracker tracker;

    @Autowired
    Alerts alerts;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void clean() {
        // Report the node empty so the tracker forgets anything a previous test left idle.
        tracker.observe(new Heartbeat(NODE, "0.1.0", List.of(), List.of()), START.minusSeconds(1));
        jdbc.update("delete from alert");
        jdbc.update("delete from process_owner");
        jdbc.update("delete from gpu_sample_1m");
        jdbc.update("delete from gpu");
        tracker.refresh();
    }

    private static Heartbeat heartbeat(int utilization, Heartbeat.Process... processes) {
        var gpu = new Heartbeat.Gpu(0, GPU, "RTX 4090", utilization, 20000, 24564, 50, 100.0, List.of(processes));
        return new Heartbeat(NODE, "0.1.0", List.of(gpu), List.of());
    }

    private static Heartbeat.Process training(int pid) {
        return new Heartbeat.Process(pid, "compute", "alice", "python train.py", 19000L);
    }

    private int alertCount(String where) {
        return jdbc.queryForObject("select count(*) from alert where " + where, Integer.class);
    }

    @Test
    void idleProcessOpensExactlyOneAlertAndActivityClosesIt() {
        tracker.observe(heartbeat(0, training(42)), START);
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(7199));
        assertThat(alertCount("true")).isZero();
        assertThat(tracker.flags(GPU, START.plusSeconds(7199)).baskingSince()).isEqualTo(START);

        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(7200));
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(7203));
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(20000));
        assertThat(alertCount("closed_at is null")).isEqualTo(1);
        assertThat(alertCount("true")).isEqualTo(1);

        tracker.observe(heartbeat(90, training(42)), START.plusSeconds(20003));
        assertThat(alertCount("closed_at is null")).isZero();
        assertThat(tracker.flags(GPU, START.plusSeconds(20003)).baskingSince()).isNull();

        // Going idle again starts a new episode with its own clock.
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(20006));
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(27205));
        assertThat(alertCount("true")).isEqualTo(1);
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(27206));
        assertThat(alertCount("true")).isEqualTo(2);
    }

    @Test
    void desktopSessionsAndSmallProcessesNeverAlert() {
        var xorg = new Heartbeat.Process(7, "graphics", "gdm", "Xorg", 4000L);
        var small = new Heartbeat.Process(8, "compute", "alice", "python", 200L);
        tracker.observe(heartbeat(0, xorg, small), START);
        tracker.observe(heartbeat(0, xorg, small), START.plusSeconds(90000));
        assertThat(alertCount("true")).isZero();
    }

    @Test
    void reservedGpuNeverAlerts() {
        jdbc.update("insert into node (name, last_seen) values (?, now()) on conflict do nothing", NODE);
        jdbc.update("insert into gpu (uuid, node, index, name, memory_total_mb, reserved) values (?, ?, 0, 'RTX 4090', 24564, true)",
                GPU, NODE);
        tracker.refresh();

        tracker.observe(heartbeat(0, training(42)), START);
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(90000));
        assertThat(alertCount("true")).isZero();
        assertThat(tracker.flags(GPU, START).reserved()).isTrue();
    }

    @Test
    void claimRoutesTheNextAlertToTheOwnerUntilTheProcessExits() {
        tracker.observe(heartbeat(0, training(42)), START);
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(7200));
        Alerts.Alert first = alerts.findOpen().get(0);
        assertThat(first.ownerSlackId()).isNull();
        alerts.claim(first, "U123");
        tracker.refresh();

        // Busy, then idle again: the new episode already knows its owner.
        tracker.observe(heartbeat(90, training(42)), START.plusSeconds(7203));
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(7206));
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(14406));
        assertThat(alerts.findOpen().get(0).ownerSlackId()).isEqualTo("U123");

        // The process exits; a later process reusing the pid is nobody's.
        tracker.observe(heartbeat(0), START.plusSeconds(14409));
        tracker.refresh();
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(14412));
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(21612));
        assertThat(alerts.findOpen().get(0).ownerSlackId()).isNull();
    }

    @Test
    void snoozedAlertComesDueOnce() {
        tracker.observe(heartbeat(0, training(42)), START);
        tracker.observe(heartbeat(0, training(42)), START.plusSeconds(7200));
        long id = alerts.findOpen().get(0).id();

        alerts.remindAt(id, START.plusSeconds(14400));
        assertThat(alerts.takeDueReminders(START.plusSeconds(14399))).isEmpty();
        assertThat(alerts.takeDueReminders(START.plusSeconds(14400))).extracting(Alerts.Alert::id).containsExactly(id);
        assertThat(alerts.takeDueReminders(START.plusSeconds(14401))).isEmpty();
    }
}
