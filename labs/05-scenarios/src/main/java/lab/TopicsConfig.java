package lab;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topic cho mọi tình huống. Bạn là người tạo topic nên RF và min.insync.replicas đặt tường minh.
 * Topic đã tồn tại thì Boot không sửa config của nó (research/0002): muốn đổi, xoá topic rồi chạy lại.
 * DLT (s8-poison-dlt) phải có ít nhất số partition của topic gốc, vì DeadLetterPublishingRecoverer gửi cùng số partition.
 */
@Configuration
public class TopicsConfig {

    @Value("${lab.replicas:3}")
    int replicas;
    @Value("${lab.min-isr:2}")
    String minIsr;

    private TopicBuilder base(String name, int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr);
    }

    @Bean NewTopic s6() { return base("s6-autocommit", 1).build(); }
    @Bean NewTopic s7() { return base("s7-poll", 1).build(); }
    @Bean NewTopic s8() { return base("s8-poison", 3).build(); }
    @Bean NewTopic s8dlt() { return base("s8-poison-dlt", 3).build(); }
    @Bean NewTopic s9() { return base("s9-points", 1).build(); }
    @Bean NewTopic s10in() { return base("s10-in", 1).build(); }
    @Bean NewTopic s10out() { return base("s10-out", 1).build(); }
    @Bean NewTopic s11default() { return base("s11-big-default", 1).build(); }

    @Bean
    NewTopic s11ok() {
        return base("s11-big-ok", 1).config(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "3000000").build();
    }

    @Bean NewTopic s12() { return base("s12-keys", 3).build(); }

    @Bean
    NewTopic s13() {
        // Cấu hình compaction "nóng" để thấy hiệu quả trong lab; production không đặt như vậy.
        return base("s13-compact", 1)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                .config(TopicConfig.SEGMENT_MS_CONFIG, "1000")
                .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")
                .config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0")
                .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "1000")
                .build();
    }
}
