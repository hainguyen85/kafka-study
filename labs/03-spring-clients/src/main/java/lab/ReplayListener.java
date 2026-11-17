package lab;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

/**
 * Đọc lại (replay) từ một mốc thời gian bằng Spring Kafka (Bài 8c).
 * Khi được giao partition LẦN ĐẦU, nhảy tới record đầu tiên có timestamp >= (bây giờ - lab.replay-minutes phút),
 * dùng seekToTimestamp (bên dưới là offsetsForTimes của Kafka client). In timestamp theo giờ Việt Nam.
 *
 * Chạy: --spring.profiles.active=replay --lab.group=g-replay [--lab.replay-minutes=5]
 *
 * Bẫy (research/0007): onPartitionsAssigned chạy MỖI LẦN được giao partition, kể cả sau rebalance.
 * Seek vô điều kiện ở đây thì mỗi lần rebalance lại đọc lại từ đầu mốc. Cờ seekedOnce giữ cho việc seek chỉ xảy ra một lần.
 */
@Component
@Profile("replay")
public class ReplayListener extends AbstractConsumerSeekAware {

    private static final DateTimeFormatter VN =
            DateTimeFormatter.ofPattern("HH:mm:ss dd/MM").withZone(ZoneId.of("Asia/Ho_Chi_Minh"));

    @Value("${lab.replay-minutes:5}")
    long replayMinutes;

    private final AtomicBoolean seekedOnce = new AtomicBoolean();

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback); // giữ callback theo partition cho AbstractConsumerSeekAware
        if (!seekedOnce.getAndSet(true)) {
            long from = System.currentTimeMillis() - replayMinutes * 60_000;
            System.out.printf("[replay] seekToTimestamp(%s) cho %s%n", VN.format(Instant.ofEpochMilli(from)), assignments.keySet());
            callback.seekToTimestamp(assignments.keySet(), from);
        } else {
            System.out.printf("[replay] được giao lại %s sau rebalance: không seek nữa%n", assignments.keySet());
        }
    }

    @KafkaListener(topics = "${lab.topic}", groupId = "${lab.group:g-replay}")
    void onMessage(ConsumerRecord<String, String> rec) {
        System.out.printf("[replay] %s %s partition=%d offset=%d key=%s%n",
                VN.format(Instant.ofEpochMilli(rec.timestamp())), rec.timestampType(),
                rec.partition(), rec.offset(), rec.key());
    }
}
