package lab;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Bạn là người tạo topic, nên partition, RF và min.insync.replicas được đặt tường minh ở đây.
 * Ba topic giống hệt nhau, chỉ khác min.insync.replicas:
 *   lab-orders-risky: min.insync.replicas=1 (chỉ cần leader còn sống là ghi được)
 *   lab-orders-safe : min.insync.replicas=2 (cần ít nhất 2 bản sao trong ISR mới ghi được khi acks=all)
 *   lab-orders-strict: min.insync.replicas=3 (mất 1 broker là ngừng nhận ghi: đánh đổi sẵn sàng lấy bền)
 * Lưu ý (research/0002): nếu topic đã tồn tại, Boot KHÔNG sửa config của nó trừ khi bật
 * spring.kafka.admin.modify-topic-configs=true. Muốn đổi config: xoá topic rồi chạy lại.
 * TopicBuilder không có minInsyncReplicas(), phải dùng .config(...).
 */
@Configuration
public class TopicsConfig {

    @Bean
    NewTopic riskyTopic() {
        return TopicBuilder.name("lab-orders-risky").partitions(3).replicas(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1").build();
    }

    @Bean
    NewTopic safeTopic() {
        return TopicBuilder.name("lab-orders-safe").partitions(3).replicas(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2").build();
    }

    @Bean
    NewTopic strictTopic() {
        return TopicBuilder.name("lab-orders-strict").partitions(3).replicas(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3").build();
    }
}
