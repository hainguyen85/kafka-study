package lab;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * S8retry: NON-BLOCKING retry bằng @RetryableTopic (so sánh với s8dlt là blocking retry).
 *
 * Gửi 8 message cùng key "acc-1" (cùng partition, đúng thứ tự) vào "s8r-orders":
 *   m3 = "FLAKY"  : lỗi tạm thời, lần giao đầu ném IllegalStateException, lần sau thành công;
 *   m5 = "POISON" : lỗi vĩnh viễn, lần nào cũng ném IllegalArgumentException.
 * @RetryableTopic(attempts = "3", backOff = 1 giây): record lỗi được gửi sang topic retry, partition gốc chạy tiếp ngay.
 * Hết 3 lần giao thì record sang topic DLT và tới phương thức @DltHandler.
 *
 * Điều cần thấy trong kết quả:
 *   1. Thứ tự đã xử lý: m4 (và các message sau) được xử lý TRƯỚC FLAKY, dù cùng key: non-blocking làm vỡ thứ tự theo key.
 *   2. POISON được giao 3 lần (IllegalArgumentException không nằm trong danh sách lỗi fatal nên vẫn bị retry),
 *      rồi vào DLT. Muốn đi thẳng DLT: thêm exclude = IllegalArgumentException.class (hoặc dùng lỗi fatal).
 *   3. Các topic được tạo tự động: tên topic retry và DLT (hậu tố mặc định -retry, -dlt).
 * Topic retry/DLT tự tạo dùng replicationFactor của annotation; ở đây lấy ${lab.replicas} cho khớp cụm.
 */
@Component
@Profile("s8retry")
public class S8Retry {
    static final String TOPIC = "s8r-orders";

    private final Run run;
    private final KafkaTemplate<String, String> template;
    private final ConfigurableApplicationContext ctx;
    private final Environment env;
    private final List<String> order = new CopyOnWriteArrayList<>();
    private final List<String> deadLetters = new CopyOnWriteArrayList<>();
    private final Map<String, Integer> deliveries = new ConcurrentHashMap<>();
    private final long t0 = System.currentTimeMillis();

    public S8Retry(Run run, KafkaTemplate<String, String> template, ConfigurableApplicationContext ctx, Environment env) {
        this.run = run;
        this.template = template;
        this.ctx = ctx;
        this.env = env;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        for (int i = 0; i < 8; i++) {
            String v = i == 3 ? "FLAKY" : i == 5 ? "POISON" : "m" + i;
            template.send(TOPIC, "acc-1", run.tag(v));
        }
        template.flush();
        Lab.say("S8retry: đã gửi m0..m7 cùng key acc-1 (m3 = FLAKY, m5 = POISON); chờ 25 giây");
        Thread t = new Thread(() -> {
            Lab.sleep(25_000);
            report();
            ctx.close();
        }, "s8retry-report");
        t.setDaemon(true);
        t.start();
    }

    @RetryableTopic(attempts = "3", backOff = @BackOff(delay = 1000), kafkaTemplate = "kafkaTemplate",
            numPartitions = "3", replicationFactor = "${lab.replicas:3}")
    @KafkaListener(topics = TOPIC, groupId = "s8r-#{@run.id}")
    void on(ConsumerRecord<String, String> rec) {
        if (!run.mine(rec.value())) {
            return;
        }
        String v = run.untag(rec.value());
        int n = deliveries.merge(v, 1, Integer::sum);
        Lab.say("+%5d ms  %-6s lần %d  từ %s-%d@%d", System.currentTimeMillis() - t0, v, n, rec.topic(), rec.partition(), rec.offset());
        if ("POISON".equals(v)) {
            throw new IllegalArgumentException("dữ liệu sai: " + v);
        }
        if ("FLAKY".equals(v) && n == 1) {
            throw new IllegalStateException("DB timeout (giả lập lỗi tạm thời)");
        }
        order.add(v);
    }

    @DltHandler
    void dlt(ConsumerRecord<String, String> rec) {
        if (!run.mine(rec.value())) {
            return;
        }
        String v = run.untag(rec.value());
        deadLetters.add(v);
        Lab.say("+%5d ms  DLT nhận %s từ %s-%d@%d", System.currentTimeMillis() - t0, v, rec.topic(), rec.partition(), rec.offset());
    }

    private void report() {
        System.out.println();
        System.out.println("===== S8retry: KẾT QUẢ =====");
        System.out.println("thứ tự đã xử lý thành công: " + order);
        int flaky = order.indexOf("FLAKY"), m4 = order.indexOf("m4");
        System.out.println(flaky > m4 && m4 >= 0
                ? "=> m4 xử lý TRƯỚC FLAKY dù cùng key: non-blocking retry làm vỡ thứ tự theo key."
                : "=> thứ tự giữ nguyên (không như mong đợi; xem log thời gian ở trên).");
        System.out.println("số lần giao: " + new java.util.TreeMap<>(deliveries));
        System.out.println("trong DLT  : " + deadLetters);
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("spring.kafka.bootstrap-servers"));
        try (Admin admin = Admin.create(p)) {
            TreeSet<String> names = new TreeSet<>(admin.listTopics().names().get());
            names.removeIf(n -> !n.startsWith(TOPIC));
            System.out.println("topic liên quan: " + names);
        } catch (Exception e) {
            System.out.println("không liệt kê được topic: " + e);
        }
    }
}
