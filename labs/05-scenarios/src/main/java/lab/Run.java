package lab;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Mã của lần chạy này. Mỗi lần chạy dùng group.id riêng ("sX-<id>") và gắn "<id>|" vào đầu value,
 * để dữ liệu còn lại của lần chạy trước không làm sai số liệu.
 *
 * Hai phase của cùng một tình huống (crash/resume, before/after) cần dùng chung mã: phase "resume" và "after"
 * đọc lại mã từ out/run-id.txt; mọi trường hợp khác tạo mã mới.
 */
@Component("run")
public class Run {

    private final String id;
    private final String phase;

    public Run(Environment env) {
        this.phase = env.getProperty("lab.phase", "");
        Path file = Path.of("out", "run-id.txt");
        try {
            if (("resume".equals(phase) || "after".equals(phase)) && Files.exists(file)) {
                this.id = Files.readString(file).trim();
            } else {
                this.id = Long.toString(System.currentTimeMillis() % 100_000_000L);
                Files.createDirectories(file.getParent());
                Files.writeString(file, id);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Dùng trong SpEL của @KafkaListener: groupId = "s8-#{@run.id}". */
    public String getId() {
        return id;
    }

    public String phase() {
        return phase;
    }

    /** Đóng gói payload thành value có mã lần chạy. */
    public String tag(String payload) {
        return id + "|" + payload;
    }

    /** Value có thuộc lần chạy này không? */
    public boolean mine(String value) {
        return value != null && value.startsWith(id + "|");
    }

    /** Bỏ mã lần chạy. */
    public String untag(String value) {
        return value.substring(id.length() + 1);
    }
}
