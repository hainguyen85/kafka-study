package lab;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topic dùng chung với Bài 3 (wizard đã tạo sẵn với cùng cấu hình). Topic đã tồn tại thì Spring bỏ qua bean này
 * và KHÔNG sửa cấu hình của nó (research/0002 mục 2.6); sửa topic đã có thì dùng kafka-configs.sh (Bài 3, bước 3b).
 */
@Configuration
public class TopicsConfig {

    @Bean
    NewTopic labOrders() {
        return TopicBuilder.name("lab-orders")
                .partitions(3)
                .replicas(3)
                .config("min.insync.replicas", "2") // TopicBuilder không có minInsyncReplicas()
                .build();
    }
}
