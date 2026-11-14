package hyrax.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest
@Import(TestDatabase.class)
class RollupsTest {

    private static final Instant MINUTE = Instant.parse("2026-11-14T10:00:00Z");

    @Autowired
    Rollups rollups;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void clean() {
        jdbc.update("delete from gpu_sample_1m");
    }

    private static Heartbeat heartbeat(Integer utilization, long memoryUsedMb, long computeMb, long graphicsMb) {
        var processes = List.of(
                new Heartbeat.Process(1, "compute", "alice", "python train.py", computeMb),
                new Heartbeat.Process(2, "graphics", "gdm", "Xorg", graphicsMb));
        var gpu = new Heartbeat.Gpu(0, "GPU-rollup", "RTX 4090", utilization, memoryUsedMb, 24564, 50, 100.0, processes);
        return new Heartbeat("node-9", "0.1.0", List.of(gpu), List.of());
    }

    @Test
    void oneRowPerGpuPerFinishedMinute() {
        rollups.add(heartbeat(10, 1000, 900, 100), MINUTE.plusSeconds(3));
        rollups.add(heartbeat(90, 3000, 2900, 100), MINUTE.plusSeconds(6));
        rollups.add(heartbeat(50, 5000, 4900, 100), MINUTE.plusSeconds(63));

        rollups.flush(MINUTE.plusSeconds(60));

        // The second minute is still open, so only the first is stored.
        List<Map<String, Object>> rows = jdbc.queryForList("select * from gpu_sample_1m where gpu_uuid = 'GPU-rollup'");
        assertThat(rows).hasSize(1);
        Map<String, Object> row = rows.get(0);
        assertThat(row.get("samples")).isEqualTo(2);
        assertThat(row.get("utilization_avg")).isEqualTo(50f);
        assertThat(row.get("utilization_max")).isEqualTo(90);
        assertThat(row.get("memory_used_avg_mb")).isEqualTo(2000f);
        assertThat(row.get("memory_used_max_mb")).isEqualTo(3000L);
        assertThat(row.get("compute_memory_max_mb")).isEqualTo(2900L);
        assertThat(jdbc.queryForObject("select node from gpu where uuid = 'GPU-rollup'", String.class))
                .isEqualTo("node-9");

        // Flushing again stores the second minute and does not repeat the first.
        rollups.flush(MINUTE.plusSeconds(120));
        assertThat(jdbc.queryForObject("select count(*) from gpu_sample_1m", Integer.class)).isEqualTo(2);
    }

    @Test
    void purgeDeletesOnlyOldRows() {
        rollups.add(heartbeat(10, 1000, 900, 100), MINUTE);
        rollups.add(heartbeat(10, 1000, 900, 100), MINUTE.plusSeconds(3600));
        rollups.flush(MINUTE.plusSeconds(7200));

        assertThat(rollups.purge(MINUTE.plusSeconds(60))).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from gpu_sample_1m", Integer.class)).isEqualTo(1);
    }
}
