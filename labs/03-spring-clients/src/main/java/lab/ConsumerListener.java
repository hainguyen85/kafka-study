package lab;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

/**
 * Consumer: in mỗi record (partition, offset, key) và mọi lần group chia lại partition.
 * Chạy: --spring.profiles.active=consumer --lab.instance=c1 [--lab.group=g1 --lab.process-ms=0]
 * Chạy 2-3 bản cùng lab.group (mỗi bản một lab.instance) để thấy group chia partition (Bài 1, Bài 5b).
 *
 * Commit offset: không cấu hình gì thì Spring dùng AckMode BATCH và tự commit sau khi listener xử lý xong
 * mọi record của một lần poll (Bài 5).
 */
@Configuration
@Profile("consumer")
public class ConsumerListener {

    @Value("${lab.instance:c1}")
    String instance;
    @Value("${lab.process-ms:0}")
    long processMs;

    @KafkaListener(topics = "${lab.topic}", groupId = "${lab.group:g1}")
    void onMessage(ConsumerRecord<String, String> rec) throws InterruptedException {
        System.out.printf("[%s] partition=%d offset=%d key=%s value=%s%n",
                instance, rec.partition(), rec.offset(), rec.key(), rec.value());
        if (processMs > 0) {
            Thread.sleep(processMs); // giả lập xử lý chậm
        }
    }

    /** Boot tự gắn bean này vào listener container (research/0002 mục 2.2). */
    @Bean
    ConsumerAwareRebalanceListener rebalanceLogger() {
        return new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                System.out.printf("[%s] REBALANCE: bị thu hồi %s (container sắp commit offset đang chờ)%n", instance, partitions);
            }

            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                System.out.printf("[%s] REBALANCE: được giao %s%n", instance, partitions);
            }

            @Override
            public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                System.out.printf("[%s] REBALANCE: MẤT %s (đã bị loại khỏi group, không commit được)%n", instance, partitions);
            }
        };
    }
}
