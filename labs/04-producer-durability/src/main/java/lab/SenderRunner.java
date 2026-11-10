package lab;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Gửi count message có số thứ tự 0..count-1, ghi lại số nào được broker báo "thành công" (ack).
 * Điều cần kiểm: số nào đã được ack mà sau đó không còn trong topic = mất message sau khi báo thành công.
 * Kết quả ack ghi vào out/<topic>-acked.txt để profile "verify" đối chiếu.
 *
 * Gửi bất đồng bộ và đọc kết quả qua future (whenComplete): nếu không đọc future thì lỗi gửi chỉ hiện trong log
 * (research/0002 mục 2.1).
 */
@Component
@Profile({"risky", "safe", "strict"})
public class SenderRunner implements ApplicationRunner {

    private final KafkaTemplate<String, String> template;

    @Value("${lab.topic}")
    String topic;
    @Value("${lab.count:100000}")
    int count;
    @Value("${lab.per-second:5000}")
    int perSecond;

    public SenderRunner(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Set<Integer> acked = ConcurrentHashMap.newKeySet();
        LongAdder failed = new LongAdder();
        Map<String, LongAdder> failuresByType = new ConcurrentHashMap<>();
        int chunk = Math.max(1, perSecond / 20); // gửi từng cụm, nghỉ 50ms giữa các cụm

        System.out.printf("[sender] topic=%s count=%d per-second=%d. Bây giờ có thể kill broker.%n", topic, count, perSecond);
        for (int seq = 0; seq < count; seq++) {
            final int s = seq;
            String key = "order-" + (seq % 30);
            try {
                template.send(topic, key, Integer.toString(seq)).whenComplete((result, ex) -> {
                    if (ex == null) {
                        acked.add(s);
                    } else {
                        record(failed, failuresByType, ex);
                    }
                });
            } catch (Exception e) { // send() có thể ném ngay (ví dụ chờ metadata quá max.block.ms)
                record(failed, failuresByType, e);
            }
            if ((seq + 1) % chunk == 0) {
                Thread.sleep(50);
            }
            if ((seq + 1) % perSecond == 0) {
                System.out.printf("[sender] đã gửi %d, acked=%d, failed=%d%n", seq + 1, acked.size(), failed.sum());
            }
        }
        template.flush();

        // Chờ mọi future có kết quả (thành công hoặc lỗi). delivery.timeout.ms chặn trên thời gian chờ.
        long deadline = System.currentTimeMillis() + 150_000;
        while (acked.size() + failed.sum() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }

        Path out = Path.of("out", topic + "-acked.txt");
        Files.createDirectories(out.getParent());
        Files.write(out, new TreeSet<>(acked).stream().map(String::valueOf).collect(Collectors.toList()));

        System.out.println();
        System.out.println("===== KẾT QUẢ GỬI =====");
        System.out.printf("topic              : %s%n", topic);
        System.out.printf("đã gửi             : %d%n", count);
        System.out.printf("được ack (thành công): %d%n", acked.size());
        System.out.printf("lỗi                : %d%n", failed.sum());
        failuresByType.forEach((type, n) -> System.out.printf("  - %s: %d%n", type, n.sum()));
        System.out.printf("chưa có kết quả    : %d%n", count - acked.size() - failed.sum());
        System.out.printf("ghi danh sách ack  : %s%n", out.toAbsolutePath());
        System.out.println("Bước tiếp: chạy profile 'verify' (sau khi bật lại broker đã kill) để đếm message mất sau khi ack.");
    }

    private static void record(LongAdder failed, Map<String, LongAdder> byType, Throwable ex) {
        failed.increment();
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        byType.computeIfAbsent(root.getClass().getSimpleName(), k -> new LongAdder()).increment();
    }
}
