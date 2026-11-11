package lab;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * S7: xử lý chậm hơn max.poll.interval.ms => consumer bị coi là chết, rebalance, record bị giao lại (TRÙNG).
 *
 * application-s7.yml: max-poll-records=3, max-poll-interval=10s; mỗi record xử lý 12 giây (>10s).
 * Quan sát: log rebalance của container, cùng offset xuất hiện nhiều lần, commit thất bại.
 * Sửa thử (chạy lại):  --spring.kafka.consumer.max-poll-records=1 --spring.kafka.consumer.max-poll-interval=30s
 */
@Component
@Profile("s7")
public class S7PollInterval {
    private static final String TOPIC = "s7-poll";

    private final Run run;
    private final KafkaTemplate<String, String> template;
    private final ConfigurableApplicationContext ctx;
    private final Map<Long, Integer> times = new ConcurrentHashMap<>();

    @Value("${lab.sleep-ms:12000}")
    long sleepMs;
    @Value("${lab.run-seconds:100}")
    long runSeconds;

    public S7PollInterval(Run run, KafkaTemplate<String, String> template, ConfigurableApplicationContext ctx) {
        this.run = run;
        this.template = template;
        this.ctx = ctx;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        for (int i = 0; i < 6; i++) {
            template.send(TOPIC, "k", run.tag("job-" + i));
        }
        template.flush();
        Lab.say("S7: đã gửi 6 job; mỗi job xử lý %d ms; chạy %d giây rồi báo cáo", sleepMs, runSeconds);
        Thread t = new Thread(() -> {
            Lab.sleep(runSeconds * 1000);
            report();
            ctx.close();
        }, "s7-report");
        t.setDaemon(true);
        t.start();
    }

    @KafkaListener(topics = TOPIC, groupId = "s7-#{@run.id}")
    void on(ConsumerRecord<String, String> rec) {
        if (!run.mine(rec.value())) {
            return;
        }
        int n = times.merge(rec.offset(), 1, Integer::sum);
        Lab.say("bắt đầu %s (offset %d, lần thứ %d)", run.untag(rec.value()), rec.offset(), n);
        Lab.sleep(sleepMs);
        Lab.say("xong %s (offset %d)", run.untag(rec.value()), rec.offset());
    }

    private void report() {
        System.out.println();
        System.out.println("===== S7: KẾT QUẢ =====");
        times.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEach(e -> System.out.printf("offset %d được xử lý %d lần%s%n", e.getKey(), e.getValue(), e.getValue() > 1 ? "  <= TRÙNG" : ""));
    }
}
