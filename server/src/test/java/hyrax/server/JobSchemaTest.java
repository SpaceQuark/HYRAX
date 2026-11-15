package hyrax.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

/** The constraints the scheduler relies on, checked against the real migrations. */
@SpringBootTest
@Import(TestDatabase.class)
class JobSchemaTest {

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void seed() {
        jdbc.update("delete from gpu_allocation");
        jdbc.update("delete from job_attempt");
        jdbc.update("delete from job");
        jdbc.update("delete from app_user");
        jdbc.update("insert into node (name, last_seen) values ('schema-node', now()) on conflict do nothing");
        jdbc.update("""
                insert into gpu (uuid, node, index, name, memory_total_mb)
                values ('GPU-schema-0', 'schema-node', 0, 'RTX 4090', 24564), ('GPU-schema-1', 'schema-node', 1, 'RTX 4090', 24564)
                on conflict do nothing
                """);
        jdbc.update("insert into app_user (name) values ('ada')");
    }

    private long job() {
        return jdbc.queryForObject("""
                insert into job (user_id, command) select id, array['python', 'train.py'] from app_user returning id
                """, Long.class);
    }

    private void attempt(long job, int attempt, String... gpus) {
        jdbc.update("""
                insert into job_attempt (job_id, attempt, node, lease_expires_at)
                values (?, ?, 'schema-node', now() + interval '30 seconds')
                """, job, attempt);
        for (String gpu : gpus) {
            jdbc.update("insert into gpu_allocation (job_id, attempt, gpu_uuid) values (?, ?, ?)", job, attempt, gpu);
        }
    }

    @Test
    void aGpuCannotBeHeldByTwoLiveAttempts() {
        long first = job();
        long second = job();
        attempt(first, 1, "GPU-schema-0", "GPU-schema-1");

        assertThatThrownBy(() -> attempt(second, 1, "GPU-schema-1")).isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    void aReleasedGpuCanBeAllocatedAgain() {
        long first = job();
        long second = job();
        attempt(first, 1, "GPU-schema-0");
        jdbc.update("update gpu_allocation set released_at = now() where job_id = ?", first);
        jdbc.update("update job_attempt set state = 'SUCCEEDED' where job_id = ?", first);

        attempt(second, 1, "GPU-schema-0");

        assertThat(jdbc.queryForObject(
                "select job_id from gpu_allocation where gpu_uuid = 'GPU-schema-0' and released_at is null", Long.class))
                .isEqualTo(second);
    }

    @Test
    void aJobHasOneLiveAttemptAndALostOneStillCounts() {
        long job = job();
        attempt(job, 1);
        jdbc.update("update job_attempt set state = 'LOST' where job_id = ?", job);

        assertThatThrownBy(() -> attempt(job, 2)).isInstanceOf(DuplicateKeyException.class);

        jdbc.update("update job_attempt set state = 'ABANDONED' where job_id = ? and attempt = 1", job);
        attempt(job, 2);
    }
}
