package lab;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Đo thông lượng và độ trễ phía producer (Bài 8b).
 * Gửi lab.count message, mỗi message lab.size byte, key xoay vòng qua lab.key-count key, nhanh nhất có thể.
 * In: số message/giây, MB/giây, và độ trễ gửi (từ lúc gọi send() tới lúc broker ack) p50/p99/max.
 *
 * Chạy (topic lab-perf do TopicsConfig tạo):
 *   --spring.profiles.active=perf --lab.topic=lab-perf
 * Đổi cấu hình producer ngay trên dòng lệnh để so sánh, ví dụ:
 *   --spring.kafka.producer.properties[linger.ms]=0
 *   --spring.kafka.producer.properties[linger.ms]=25 --spring.kafka.producer.batch-size=131072
 *   --spring.kafka.producer.compression-type=lz4
 *
 * Lưu ý: độ trễ đo ở đây là độ trễ PHÍA PRODUCER (tới lúc ack). Consumer chỉ thấy message khi mọi replica
 * trong ISR đã có nó, bất kể acks (Design: Replicated Logs).
 */
@Component
@Profile("perf")
public class PerfRunner implements ApplicationRunner {

    private final KafkaTemplate<String, String> template;

    @Value("${lab.topic}")
    String topic;
    @Value("${lab.count:200000}")
    int count;
    @Value("${lab.size:200}")
    int size;
    @Value("${lab.key-count:1000}")
    int keyCount;

    public PerfRunner(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Map<String, Object> cfg = template.getProducerFactory().getConfigurationProperties();
        System.out.printf("[perf] topic=%s count=%d size=%dB | acks=%s linger.ms=%s batch.size=%s compression.type=%s%n",
                topic, count, size, cfg.get("acks"), val(cfg, "linger.ms", "5 (mặc định 4.x)"),
                val(cfg, "batch.size", "16384 (mặc định)"), val(cfg, "compression.type", "none (mặc định)"));

        String payload = "x".repeat(size);
        long[] latencyNs = new long[count];
        AtomicInteger errors = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(count);

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final int idx = i;
            final long t0 = System.nanoTime();
            // send() có thể CHẶN thread này khi buffer.memory đầy, tối đa max.block.ms (backpressure phía producer)
            template.send(topic, "k-" + (i % keyCount), payload).whenComplete((r, ex) -> {
                latencyNs[idx] = System.nanoTime() - t0;
                if (ex != null) errors.incrementAndGet();
                done.countDown();
            });
        }
        if (!done.await(5, TimeUnit.MINUTES)) {
            System.out.println("[perf] Hết 5 phút mà chưa nhận đủ kết quả gửi.");
        }
        double sec = (System.nanoTime() - start) / 1e9;

        long[] sorted = latencyNs.clone();
        Arrays.sort(sorted);
        System.out.printf("[perf] %.0f message/giây, %.1f MB/giây, lỗi=%d, tổng %.1f giây%n",
                count / sec, count * (double) size / sec / 1_000_000, errors.get(), sec);
        System.out.printf("[perf] độ trễ gửi (tới ack): p50=%.1f ms  p99=%.1f ms  max=%.1f ms%n",
                ms(sorted[count / 2]), ms(sorted[(int) (count * 0.99) - 1]), ms(sorted[count - 1]));
    }

    private static Object val(Map<String, Object> cfg, String key, String dflt) {
        Object v = cfg.get(key);
        return v != null ? v : dflt;
    }

    private static double ms(long ns) {
        return ns / 1e6;
    }
}
