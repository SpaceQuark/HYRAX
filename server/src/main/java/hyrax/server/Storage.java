package hyrax.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/** Snapshots and job outputs on disk. Every write is checked against the hash the sender claims. */
@Component
public class Storage {

    public record Stored(long bytes, String sha256) {}

    private final Path root;

    public Storage(@Value("${hyrax.data-dir}") Path root) {
        this.root = root.toAbsolutePath().normalize();
    }

    public Path snapshot(String sha256) {
        if (!sha256.matches("[0-9a-f]{64}")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "sha256 must be 64 hex digits");
        }
        return root.resolve("snapshots").resolve(sha256 + ".tar.gz");
    }

    /** The file for one output of one attempt. Rejects any path that would land outside the attempt's directory. */
    public Path artifact(long jobId, int attempt, String path) {
        Path base = root.resolve("artifacts").resolve(Long.toString(jobId)).resolve(Integer.toString(attempt));
        Path file = base.resolve(path).normalize();
        boolean unsafe = path.isBlank() || path.startsWith("/") || path.contains("\\") || path.contains("\0")
                || !file.startsWith(base) || file.equals(base);
        for (String part : path.split("/")) {
            unsafe |= part.equals("..") || part.equals(".") || part.isEmpty();
        }
        if (unsafe) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "unsafe path " + path);
        }
        return file;
    }

    /**
     * Writes the stream to {@code target} through a temporary file, so a broken upload never leaves a
     * partial file under the real name. Fails if it is longer than {@code maxBytes} or does not hash to
     * {@code expectedSha256}.
     */
    public Stored write(InputStream in, Path target, long maxBytes, String expectedSha256) throws IOException {
        Files.createDirectories(target.getParent());
        Path temporary = Files.createTempFile(target.getParent(), ".upload-", ".part");
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            long total = 0;
            try (OutputStream out = new DigestOutputStream(Files.newOutputStream(temporary), digest)) {
                byte[] buffer = new byte[1 << 16];
                for (int read; (read = in.read(buffer)) > 0; ) {
                    total += read;
                    if (total > maxBytes) {
                        throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "larger than the " + maxBytes + " byte limit");
                    }
                    out.write(buffer, 0, read);
                }
            }
            String sha256 = HexFormat.of().formatHex(digest.digest());
            if (!sha256.equals(expectedSha256)) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "content does not match its sha256");
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            return new Stored(total, sha256);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }
}
