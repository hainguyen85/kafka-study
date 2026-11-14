package lab;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Producer: gửi lab.count message, xoay vòng qua các key trong lab.keys, rồi in partition và offset mà broker trả về.
 * Chạy: --spring.profiles.active=producer [--lab.count=9 --lab.keys=DRV-001,DRV-002,DRV-003]
 *
 * Điểm cần thấy: cùng key luôn cùng partition, offset tăng dần trong từng partition (Bài 2).
 * Kết quả gửi đọc từ CompletableFuture; không đọc future thì lỗi gửi chỉ nằm trong log (Bài 4).
 */
@Component
@Profile("producer")
public class ProducerRunner implements ApplicationRunner {

    private final KafkaTemplate<String, String> template;

    @Value("${lab.topic}")
    String topic;
    @Value("${lab.count:9}")
    int count;
    @Value("${lab.keys:DRV-001,DRV-002,DRV-003}")
    List<String> keys;

    public ProducerRunner(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        for (int i = 0; i < count; i++) {
            String key = keys.get(i % keys.size());
            String value = "msg-" + i;
            CompletableFuture<SendResult<String, String>> future = template.send(topic, key, value);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    var md = result.getRecordMetadata();
                    System.out.printf("[producer] key=%-8s value=%-6s -> partition=%d offset=%d%n",
                            key, value, md.partition(), md.offset());
                } else {
                    System.out.printf("[producer] GỬI LỖI key=%s value=%s: %s%n", key, value, ex);
                }
            });
        }
        template.flush();
        // Chờ kết quả cuối cùng để in đủ trước khi ứng dụng thoát (tối đa 30 giây).
        template.send(topic, "done", "end-of-run").get(30, TimeUnit.SECONDS);
        System.out.println("[producer] xong.");
    }
}
