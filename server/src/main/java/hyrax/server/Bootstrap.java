package hyrax.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Creates the first admin, since every other user is created by an admin. */
@Component
public class Bootstrap {

    private static final Logger log = LoggerFactory.getLogger(Bootstrap.class);

    private final JdbcTemplate jdbc;
    private final String admin;
    private final Path tokenFile;

    public Bootstrap(
            JdbcTemplate jdbc,
            @Value("${hyrax.bootstrap.admin}") String admin,
            @Value("${hyrax.bootstrap.token-file}") Path tokenFile) {
        this.jdbc = jdbc;
        this.admin = admin;
        this.tokenFile = tokenFile;
    }

    @EventListener(ApplicationReadyEvent.class)
    void createFirstAdmin() throws IOException {
        if (admin.isBlank() || jdbc.queryForObject("select count(*) from app_user", Integer.class) > 0) {
            return;
        }
        String token = Auth.newToken("hxu_");
        // The file is written first: a user row whose token nobody has would block the next bootstrap.
        Files.createDirectories(tokenFile.getParent());
        Files.deleteIfExists(tokenFile);
        Files.createFile(tokenFile, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        Files.writeString(tokenFile, token + "\n");
        jdbc.update("insert into app_user (name, token_hash, is_admin) values (?, ?, true)", admin, Auth.hash(token));
        log.info("Created the first admin '{}'. Its API token is in {}", admin, tokenFile);
    }
}
