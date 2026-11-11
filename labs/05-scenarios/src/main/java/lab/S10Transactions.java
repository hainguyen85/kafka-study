package lab;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * S10: consume-transform-produce trong transaction, và vì sao consumer phải đọc read_committed.
 *
 * Đầu vào a, b, BOOM, c. Listener (chạy trong transaction do container mở, vì đã đặt transaction-id-prefix):
 * gửi bản IN HOA sang s10-out rồi, với BOOM và LẦN ĐẦU, ném lỗi => transaction abort, container rollback và giao lại BOOM.
 *
 * Kết quả: hai consumer đọc s10-out:
 *   read_uncommitted: thấy cả record của transaction đã abort => "BOOM" xuất hiện 2 lần (1 aborted + 1 committed)
 *   read_committed  : chỉ thấy record của transaction đã commit => mỗi giá trị đúng 1 lần
 * Phạm vi: exactly-once này chỉ đúng trong Kafka (đọc topic vào, ghi topic ra, commit offset cùng transaction);
 * ghi ra DB/HTTP ngoài thì phải tự bảo đảm (docs Kafka design).
 */
@Component
@Profile("s10")
public class S10Transactions {

    private final Run run;
    private final Environment env;
    private final KafkaTemplate<String, String> template;
    private final ConfigurableApplicationContext ctx;
    private final AtomicBoolean failedOnce = new AtomicBoolean(false);

    @Value("${lab.wait-seconds:15}")
    long waitSeconds;

    public S10Transactions(Run run, Environment env, KafkaTemplate<String, String> template, ConfigurableApplicationContext ctx) {
        this.run = run;
        this.env = env;
        this.template = template;
        this.ctx = ctx;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        // template có transaction-id-prefix nên mọi send phải nằm trong transaction
        template.executeInTransaction(t -> {
            for (String v : List.of("a", "b", "BOOM", "c")) {
                t.send("s10-in", "k", run.tag(v));
            }
            return true;
        });
        Lab.say("S10: đã gửi a, b, BOOM, c vào s10-in; chờ %d giây", waitSeconds);
        Thread t = new Thread(() -> {
            Lab.sleep(waitSeconds * 1000);
            report();
            ctx.close();
        }, "s10-report");
        t.setDaemon(true);
        t.start();
    }

    @KafkaListener(topics = "s10-in", groupId = "s10-#{@run.id}")
    void on(String value) {
        if (!run.mine(value)) {
            return;
        }
        String v = run.untag(value);
        try {
            // .get(): chờ record THỰC SỰ tới broker. Nếu không chờ, khi ném lỗi producer hủy lô chưa gửi
            // ("TransactionAbortedException: Failing batch since transaction was aborted") và không có bản ghi aborted nào để thấy.
            template.send("s10-out", "k", run.tag(v.toUpperCase())).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        if ("BOOM".equals(v) && failedOnce.compareAndSet(false, true)) {
            Lab.say("ném lỗi sau khi đã gửi output của BOOM => transaction abort");
            throw new IllegalStateException("lỗi giả lập sau khi đã gửi output");
        }
    }

    private void report() {
        String bs = env.getProperty("spring.kafka.bootstrap-servers");
        System.out.println();
        System.out.println("===== S10: KẾT QUẢ (đọc topic s10-out) =====");
        for (String level : List.of("read_uncommitted", "read_committed")) {
            List<String> vals = Lab.readAll(bs, "s10-out", level, null).stream()
                    .filter(r -> run.mine(r.value())).map(r -> run.untag(r.value())).collect(Collectors.toList());
            System.out.printf("%-17s: %s%n", level, vals);
        }
        System.out.println("Mong đợi: read_uncommitted thấy BOOM 2 lần; read_committed thấy A, B, BOOM, C mỗi giá trị 1 lần.");
    }
}
