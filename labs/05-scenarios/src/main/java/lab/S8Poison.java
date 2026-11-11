package lab;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

/**
 * S8: poison pill (message làm listener ném lỗi mãi).
 *
 * Gửi 10 message m0..m9 (cùng key => cùng partition, giữ thứ tự), trong đó m5 = "POISON" làm listener ném lỗi.
 *
 * Profile s8    : KHÔNG cấu hình gì thêm. DefaultErrorHandler mặc định thử lại tối đa 10 lần giao (không chờ),
 *                 rồi chỉ ghi log ERROR và commit offset => POISON BỊ BỎ QUA IM LẶNG, không ở đâu cả (research/0002).
 * Profile s8dlt : thêm DefaultErrorHandler + DeadLetterPublishingRecoverer + FixedBackOff(300ms, 2 lần thử lại):
 *                 POISON được chuyển sang topic "s8-poison-dlt" cùng số partition, kèm header mô tả lỗi.
 * Không có pattern DLQ "chính thức" cho consumer thường trong docs Apache Kafka (chỉ Connect và Share Groups);
 * đây là tính năng của Spring Kafka.
 */
@Component
@Profile({"s8", "s8dlt"})
public class S8Poison {
    private static final String TOPIC = "s8-poison";

    private final Run run;
    private final KafkaTemplate<String, String> template;
    private final ConfigurableApplicationContext ctx;
    private final List<String> processed = new CopyOnWriteArrayList<>();
    private final List<String> deadLetters = new CopyOnWriteArrayList<>();

    public S8Poison(Run run, KafkaTemplate<String, String> template, ConfigurableApplicationContext ctx) {
        this.run = run;
        this.template = template;
        this.ctx = ctx;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        for (int i = 0; i < 10; i++) {
            template.send(TOPIC, "k", run.tag(i == 5 ? "POISON" : "m" + i));
        }
        template.flush();
        Lab.say("S8: đã gửi 10 message (m5 = POISON); chờ 25 giây");
        Thread t = new Thread(() -> {
            Lab.sleep(25_000);
            report();
            ctx.close();
        }, "s8-report");
        t.setDaemon(true);
        t.start();
    }

    @KafkaListener(topics = TOPIC, groupId = "s8-#{@run.id}")
    void on(ConsumerRecord<String, String> rec) {
        if (!run.mine(rec.value())) {
            return;
        }
        String v = run.untag(rec.value());
        if ("POISON".equals(v)) {
            Lab.say("listener gặp POISON (offset %d) => ném lỗi", rec.offset());
            throw new IllegalArgumentException("không parse được nghiệp vụ: " + v);
        }
        processed.add(v);
    }

    /** Luôn nghe DLT; ở profile s8 sẽ không có gì đến. */
    @KafkaListener(topics = "s8-poison-dlt", groupId = "s8dlt-#{@run.id}")
    void onDlt(ConsumerRecord<String, String> rec) {
        if (!run.mine(rec.value())) {
            return;
        }
        deadLetters.add(run.untag(rec.value()));
        Lab.say("DLT nhận %s (partition %d)", run.untag(rec.value()), rec.partition());
    }

    private void report() {
        System.out.println();
        System.out.println("===== S8: KẾT QUẢ =====");
        System.out.println("đã xử lý : " + processed);
        System.out.println("trong DLT: " + deadLetters);
        boolean poisonSomewhere = deadLetters.contains("POISON");
        System.out.println(poisonSomewhere
                ? "=> POISON nằm trong DLT: còn dấu vết để điều tra/xử lý lại."
                : "=> POISON KHÔNG có ở đâu cả: đã bị bỏ qua im lặng (chỉ còn log ERROR).");
    }

    /** Chỉ có ở profile s8dlt: Boot tự gắn bean CommonErrorHandler vào container factory mặc định. */
    @org.springframework.context.annotation.Configuration
    @Profile("s8dlt")
    static class DltConfig {
        @Bean
        DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
            return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), new FixedBackOff(300L, 2L));
        }
    }
}
