package lab;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * S6: auto-commit vừa làm MẤT message vừa làm TRÙNG (xử lý bất đồng bộ + crash).
 *
 * Chạy 2 lần cho cùng một nhóm kết quả:
 *   lần 1 (--lab.phase=crash, mặc định): gửi 300 message, consumer nhận, giao việc cho thread pool, ~2,5 giây sau "crash".
 *   lần 2 (--lab.phase=resume)         : consumer chạy tiếp cùng group, đợi yên lặng 8 giây rồi in báo cáo.
 *
 * Profile s6    : auto-commit bật; listener giao việc sang thread pool rồi return ngay => offset bị commit trước khi xử lý xong.
 * Profile s6fix : tắt auto-commit, xử lý xong rồi mới ack => crash chỉ gây trùng, không mất.
 * Báo cáo: số seq (0..299) không bao giờ được xử lý = MẤT; số lần xử lý dư = TRÙNG.
 */
@Component
@Profile({"s6", "s6fix"})
public class S6AutoCommit {
    static final int N = 300;
    private static final String TOPIC = "s6-autocommit";
    private static final String FILE = "s6-processed.txt";

    private final Run run;
    private final Environment env;
    private final KafkaTemplate<String, String> template;
    private final ConfigurableApplicationContext ctx;
    private final boolean fix;
    private final ExecutorService pool = Executors.newFixedThreadPool(2);
    private volatile long firstSeen = 0;
    private volatile long lastActivity = System.currentTimeMillis();
    private final long startedAt = System.currentTimeMillis();

    public S6AutoCommit(Run run, Environment env, KafkaTemplate<String, String> template, ConfigurableApplicationContext ctx) {
        this.run = run;
        this.env = env;
        this.template = template;
        this.ctx = ctx;
        this.fix = env.acceptsProfiles(Profiles.of("s6fix"));
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        Lab.say("S6 (%s) phase=%s run=%s", fix ? "s6fix: ack thủ công" : "s6: auto-commit + xử lý bất đồng bộ", run.phase().isEmpty() ? "crash" : run.phase(), run.getId());
        if (!"resume".equals(run.phase())) {
            Lab.delete(FILE);
            for (int i = 0; i < N; i++) {
                template.send(TOPIC, "k", run.tag("seq-" + i));
            }
            template.flush();
            Lab.say("đã gửi %d message", N);
        }
        Thread t = new Thread(this::watchdog, "s6-watchdog");
        t.setDaemon(true);
        t.start();
    }

    /** Xử lý một record. ack = null ở profile s6 (không có Acknowledgment trong chế độ auto-commit). */
    void handle(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        if (!run.mine(rec.value())) {
            if (ack != null) {
                ack.acknowledge();
            }
            return;
        }
        long now = System.currentTimeMillis();
        if (firstSeen == 0) {
            firstSeen = now;
        }
        lastActivity = now;
        String seq = run.untag(rec.value());
        if (fix) {
            process(seq);
            ack.acknowledge();               // chỉ ack SAU khi đã xử lý xong
        } else {
            pool.submit(() -> process(seq)); // return ngay: offset bị auto-commit khi việc chưa xong
        }
    }

    /**
     * Hai listener tách theo profile vì: khai báo tham số Acknowledgment khi container KHÔNG ở chế độ manual làm
     * Spring ném IllegalStateException ("No Acknowledgment available as an argument, the listener container must
     * have a MANUAL AckMode") chứ không phải đưa null (đã gặp khi chạy thử).
     */
    @Component
    @Profile("s6")
    static class Unsafe {
        private final S6AutoCommit outer;

        Unsafe(S6AutoCommit outer) {
            this.outer = outer;
        }

        @KafkaListener(topics = TOPIC, groupId = "s6-#{@run.id}")
        void on(ConsumerRecord<String, String> rec) {
            outer.handle(rec, null);
        }
    }

    @Component
    @Profile("s6fix")
    static class Fixed {
        private final S6AutoCommit outer;

        Fixed(S6AutoCommit outer) {
            this.outer = outer;
        }

        @KafkaListener(topics = TOPIC, groupId = "s6-#{@run.id}")
        void on(ConsumerRecord<String, String> rec, Acknowledgment ack) {
            outer.handle(rec, ack);
        }
    }

    private void process(String seq) {
        Lab.sleep(30);
        Lab.append(FILE, seq);
    }

    private void watchdog() {
        boolean resume = "resume".equals(run.phase());
        while (true) {
            Lab.sleep(200);
            long now = System.currentTimeMillis();
            if (!resume && firstSeen > 0 && now - firstSeen > 2500) {
                Lab.say("*** CRASH giả lập: process bị chết giữa chừng ***");
                if ("close".equals(env.getProperty("lab.crash", "halt"))) {
                    pool.shutdownNow();
                    ctx.close();
                } else {
                    Runtime.getRuntime().halt(137); // như kill -9: không commit gì thêm, không dọn dẹp
                }
                return;
            }
            boolean quiet = firstSeen > 0 && now - lastActivity > 8000;
            boolean never = firstSeen == 0 && now - startedAt > 25000;
            if (resume && (quiet || never)) {
                report();
                ctx.close();
                return;
            }
        }
    }

    private void report() {
        List<String> lines = Lab.read(FILE);
        Set<String> unique = new HashSet<>(lines);
        int missing = 0;
        for (int i = 0; i < N; i++) {
            if (!unique.contains("seq-" + i)) {
                missing++;
            }
        }
        System.out.println();
        System.out.println("===== S6: KẾT QUẢ (" + (fix ? "s6fix" : "s6") + ") =====");
        System.out.printf("đã gửi              : %d%n", N);
        System.out.printf("seq khác nhau xử lý : %d%n", unique.size());
        System.out.printf("MẤT (không xử lý)   : %d%n", missing);
        System.out.printf("TRÙNG (xử lý dư)    : %d%n", lines.size() - unique.size());
    }
}
