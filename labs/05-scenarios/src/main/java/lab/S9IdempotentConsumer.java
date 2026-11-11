package lab;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * S9: trùng message dù producer đã bật idempotence (cộng điểm thưởng bị gấp đôi).
 *
 * Idempotence của producer chỉ chống trùng trong MỘT session của producer (docs KafkaProducer). Nếu ứng dụng tự gửi lại
 * (ví dụ restart rồi gửi lại lệnh vì không biết lệnh trước đã vào chưa), broker thấy hai record khác nhau.
 * Ở đây mỗi sự kiện CreditPoints được gửi 2 lần với cùng eventId, mô phỏng việc gửi lại của ứng dụng.
 *
 * Profile s9      : consumer cộng điểm mỗi lần nhận => điểm bị gấp đôi.
 * Profile s9dedup : consumer bỏ qua eventId đã xử lý => điểm đúng.
 *
 * CẢNH BÁO: tập "đã xử lý" ở đây nằm trong RAM chỉ để minh hoạ. Thực tế dedup phải nằm CÙNG giao dịch với tác dụng
 * phụ (ví dụ ràng buộc UNIQUE(eventId) trong DB, cộng điểm và ghi eventId trong cùng một transaction), nếu không
 * crash giữa hai việc vẫn gây mất hoặc trùng. Docs Spring Kafka/Apache không có tài liệu chính thức cho pattern này.
 */
@Component
@Profile({"s9", "s9dedup"})
public class S9IdempotentConsumer {
    private static final String TOPIC = "s9-points";

    private final Run run;
    private final KafkaTemplate<String, String> template;
    private final ConfigurableApplicationContext ctx;
    private final boolean dedup;
    private final Set<String> seenEvents = ConcurrentHashMap.newKeySet();
    private final Map<String, Integer> points = new ConcurrentHashMap<>();
    private int duplicatesSkipped = 0;

    public S9IdempotentConsumer(Run run, Environment env, KafkaTemplate<String, String> template, ConfigurableApplicationContext ctx) {
        this.run = run;
        this.template = template;
        this.ctx = ctx;
        this.dedup = env.acceptsProfiles(Profiles.of("s9dedup"));
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        // 5 sự kiện: user u1..u5 được cộng (10 * n) điểm; mỗi sự kiện gửi 2 lần.
        for (int n = 1; n <= 5; n++) {
            String event = "e" + n + ":u" + n + ":" + (10 * n); // eventId:user:points
            template.send(TOPIC, "u" + n, run.tag(event));
            template.send(TOPIC, "u" + n, run.tag(event)); // gửi lại bởi ứng dụng
        }
        template.flush();
        Lab.say("S9 (%s): đã gửi 5 sự kiện, mỗi sự kiện 2 lần; chờ 15 giây", dedup ? "s9dedup" : "s9");
        Thread t = new Thread(() -> {
            Lab.sleep(15_000);
            report();
            ctx.close();
        }, "s9-report");
        t.setDaemon(true);
        t.start();
    }

    @KafkaListener(topics = TOPIC, groupId = "s9-#{@run.id}")
    void on(ConsumerRecord<String, String> rec) {
        if (!run.mine(rec.value())) {
            return;
        }
        String[] f = run.untag(rec.value()).split(":");
        String eventId = f[0];
        String user = f[1];
        int pts = Integer.parseInt(f[2]);
        if (dedup && !seenEvents.add(eventId)) {
            duplicatesSkipped++;
            return; // đã xử lý eventId này
        }
        points.merge(user, pts, Integer::sum);
    }

    private void report() {
        System.out.println();
        System.out.println("===== S9: KẾT QUẢ (" + (dedup ? "s9dedup" : "s9") + ") =====");
        System.out.println("điểm mong đợi: u1=10 u2=20 u3=30 u4=40 u5=50");
        System.out.println("điểm thực tế : " + new TreeMap<>(points));
        System.out.println("bản trùng bị bỏ qua: " + duplicatesSkipped);
    }
}
