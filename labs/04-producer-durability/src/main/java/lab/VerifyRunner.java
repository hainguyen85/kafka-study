package lab;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Đọc toàn bộ topic từ đầu bằng KafkaConsumer thuần (không group, không commit offset), rồi đối chiếu với
 * danh sách seq đã được ack (out/<topic>-acked.txt) do SenderRunner ghi:
 *   - acked nhưng không có trong topic  => MẤT sau khi báo thành công (điều ta muốn thấy bằng 0)
 *   - seq xuất hiện nhiều lần          => TRÙNG
 *   - có trong topic nhưng không acked => cho phép (producer không biết kết quả nhưng broker đã ghi)
 */
@Component
@Profile("verify")
public class VerifyRunner implements ApplicationRunner {

    private final Environment env;

    @Value("${lab.topic}")
    String topic;

    public VerifyRunner(Environment env) {
        this.env = env;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Path ackedFile = Path.of("out", topic + "-acked.txt");
        Set<Integer> acked = new HashSet<>();
        for (String line : Files.readAllLines(ackedFile)) {
            if (!line.isBlank()) {
                acked.add(Integer.parseInt(line.trim()));
            }
        }

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("spring.kafka.bootstrap-servers"));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        Map<Integer, Integer> seen = new HashMap<>();
        long total = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : consumer.partitionsFor(topic)) {
                tps.add(new TopicPartition(topic, pi.partition()));
            }
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            Map<TopicPartition, Long> end = consumer.endOffsets(tps);
            while (tps.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(2))) {
                    seen.merge(Integer.parseInt(r.value()), 1, Integer::sum);
                    total++;
                }
            }
        }

        List<Integer> lost = acked.stream().filter(s -> !seen.containsKey(s)).sorted().toList();
        long duplicated = seen.values().stream().filter(n -> n > 1).count();
        long presentNotAcked = seen.keySet().stream().filter(s -> !acked.contains(s)).count();

        System.out.println();
        System.out.println("===== ĐỐI CHIẾU =====");
        System.out.printf("topic                          : %s%n", topic);
        System.out.printf("seq được ack                   : %d%n", acked.size());
        System.out.printf("record đọc được trong topic    : %d (seq khác nhau: %d)%n", total, seen.size());
        System.out.printf("ACKED NHƯNG MẤT                : %d  %s%n", lost.size(),
                lost.isEmpty() ? "" : "ví dụ: " + lost.subList(0, Math.min(15, lost.size())));
        System.out.printf("seq bị TRÙNG                   : %d%n", duplicated);
        System.out.printf("có trong topic nhưng không ack : %d (chấp nhận được)%n", presentNotAcked);
    }
}
