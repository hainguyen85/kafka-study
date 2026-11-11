package lab;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * S12: thêm partition làm key đổi partition (thứ tự theo key bị vỡ).
 *   Bước 1 (--lab.phase=before): gửi 20 key k0..k19, ghi partition của từng key.
 *   Bước 2 (bạn tự chạy):        kafka-topics.sh --alter --topic s12-keys --partitions 6
 *   Bước 3 (--lab.phase=after):  gửi lại 20 key, so sánh: key nào đổi partition.
 * Docs vận hành: "messages with the same key may be routed to different partitions after the expansion"
 * và Kafka không phân phối lại dữ liệu cũ; không giảm được số partition.
 */
@Component
@Profile("s12")
public class S12PartitionMap implements ApplicationRunner {

    private final Run run;
    private final KafkaTemplate<String, String> template;
    private final ConfigurableApplicationContext ctx;

    public S12PartitionMap(Run run, KafkaTemplate<String, String> template, ConfigurableApplicationContext ctx) {
        this.run = run;
        this.template = template;
        this.ctx = ctx;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String phase = run.phase().isEmpty() ? "before" : run.phase();
        Map<String, Integer> now = new LinkedHashMap<>();
        for (int i = 0; i < 20; i++) {
            String key = "k" + i;
            int p = template.send("s12-keys", key, run.tag("v")).get().getRecordMetadata().partition();
            now.put(key, p);
        }
        if ("before".equals(phase)) {
            Lab.delete("s12-before.txt");
            now.forEach((k, p) -> Lab.append("s12-before.txt", k + "=" + p));
            Lab.say("S12 before: %s", now);
            Lab.say("Bây giờ chạy: kafka-topics.sh --alter --topic s12-keys --partitions 6, rồi chạy lại với --lab.phase=after");
        } else {
            Map<String, Integer> before = new LinkedHashMap<>();
            for (String line : Lab.read("s12-before.txt")) {
                String[] kv = line.split("=");
                before.put(kv[0], Integer.parseInt(kv[1]));
            }
            long moved = now.entrySet().stream().filter(e -> !e.getValue().equals(before.get(e.getKey()))).count();
            System.out.println();
            System.out.println("===== S12: KẾT QUẢ =====");
            now.forEach((k, p) -> System.out.printf("%-4s trước=%s sau=%d%s%n", k, before.get(k), p,
                    p != before.get(k) ? "   <= ĐỔI PARTITION" : ""));
            System.out.printf("%d/%d key đã đổi partition: event cũ và mới của các key này không còn thứ tự chung.%n", moved, now.size());
        }
        ctx.close();
    }
}
