package hyrax.server;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/** Who is calling: an agent with its node token, or a person with a personal API token. */
@Component
public class Auth {

    public record User(long id, String name, boolean admin) {}

    private static final SecureRandom RANDOM = new SecureRandom();

    private final JdbcTemplate jdbc;
    private final String sharedAgentToken;

    public Auth(JdbcTemplate jdbc, @Value("${hyrax.agent-token}") String sharedAgentToken) {
        this.jdbc = jdbc;
        this.sharedAgentToken = sharedAgentToken;
    }

    /** A fresh token. Only its hash is stored, so it can be shown exactly once. */
    public static String newToken(String prefix) {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        return prefix + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    public static String hash(String token) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String bearer(String authorization) {
        return authorization != null && authorization.startsWith("Bearer ") ? authorization.substring(7) : null;
    }

    /**
     * Checks an agent's heartbeat. Returns true when the node proved its identity with its own token over
     * HTTPS, which is what entitles it to be given jobs. A node that is not enrolled may still report
     * telemetry with the shared token; an enrolled node may not fall back to it.
     */
    public boolean agentMayRunJobs(String authorization, String node, HttpServletRequest request) {
        String token = bearer(authorization);
        List<String> enrolled = jdbc.queryForList("select agent_token_hash from node where name = ?", String.class, node);
        String expected = enrolled.isEmpty() ? null : enrolled.get(0);
        if (expected != null) {
            if (token == null || !constantTimeEquals(expected, hash(token))) {
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
            }
            return request.isSecure();
        }
        if (!sharedAgentToken.isBlank() && (token == null || !constantTimeEquals(sharedAgentToken, token))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        return false;
    }

    /** The enrolled node this token belongs to. For agent calls that touch jobs. */
    public String requireEnrolledNode(String authorization, HttpServletRequest request) {
        requireSecure(request);
        String token = bearer(authorization);
        List<String> nodes = token == null
                ? List.of()
                : jdbc.queryForList("select name from node where agent_token_hash = ?", String.class, hash(token));
        if (nodes.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        return nodes.get(0);
    }

    public User requireUser(String authorization, HttpServletRequest request) {
        requireSecure(request);
        String token = bearer(authorization);
        List<User> users = token == null
                ? List.of()
                : jdbc.query(
                        "select id, name, is_admin from app_user where token_hash = ? and disabled_at is null",
                        (row, index) -> new User(row.getLong("id"), row.getString("name"), row.getBoolean("is_admin")),
                        hash(token));
        if (users.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "a personal API token is required");
        }
        return users.get(0);
    }

    public User requireAdmin(String authorization, HttpServletRequest request) {
        User user = requireUser(authorization, request);
        if (!user.admin()) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "admins only");
        }
        return user;
    }

    private static void requireSecure(HttpServletRequest request) {
        if (!request.isSecure()) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "jobs are only available over HTTPS");
        }
    }

    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
