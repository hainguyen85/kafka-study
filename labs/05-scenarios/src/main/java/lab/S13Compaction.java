package lab;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * S13: topic compacted làm "bảng giá mới nhất". Topic s13-compact: cleanup.policy=compact, segment.ms=1000,
 * min.cleanable.dirty.ratio=0.01, delete.retention.ms=1000 (cấu hình "nóng" chỉ để lab).
 *
 * Ghi 300 lần cập nhật giá cho "price-A", 1 giá cho "price-B" rồi tombstone (value null) cho "price-B",
 * thêm vài record phụ cách nhau 1,2 giây để segment đóng lại (compaction chỉ chạy trên segment đã đóng).
 * Đọc ngay (chưa compact) và đọc lại sau lab.wait-seconds. Compaction chạy ngầm và không có hạn chót cứng
 * ("is not a hard guarantee" theo topic configs): nếu chưa thấy giảm, tăng --lab.wait-seconds và đọc lại.
 */
@Component
@Profile("s13")
public class S13Compaction implements ApplicationRunner {
    private static final String TOPIC = "s13-compact";

    private final Run run;
    private final Environment env;
    private final KafkaTemplate<String, String> template;
    private final ConfigurableApplicationContext ctx;

    @Value("${lab.wait-seconds:90}")
    long waitSeconds;

    public S13Compaction(Run run, Environment env, KafkaTemplate<String, String> template, ConfigurableApplicationContext ctx) {
        this.run = run;
        this.env = env;
        this.template = template;
        this.ctx = ctx;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        for (int i = 0; i < 300; i++) {
            template.send(TOPIC, "price-A", run.tag("v" + i));
        }
        template.send(TOPIC, "price-B", run.tag("100")).get();
        template.send(TOPIC, "price-B", null).get(); // tombstone: xoá price-B
        for (int i = 0; i < 6; i++) {               // record phụ để các segment cũ đóng lại
            Lab.sleep(1200);
            template.send(TOPIC, "filler-" + i, run.tag("x")).get();
        }
        Lab.say("S13: đã ghi xong; đọc ngay (chưa compact)");
        summarize("TRƯỚC compaction");
        Lab.say("chờ %d giây cho log cleaner chạy...", waitSeconds);
        Lab.sleep(waitSeconds * 1000);
        summarize("SAU " + waitSeconds + "s");
        ctx.close();
    }

    private void summarize(String label) {
        List<Lab.Rec> recs = Lab.readAll(env.getProperty("spring.kafka.bootstrap-servers"), TOPIC, "read_uncommitted", null);
        Map<String, Integer> perKey = new TreeMap<>();
        int tombstones = 0;
        for (Lab.Rec r : recs) {
            if (r.value() == null) {
                tombstones++;
                perKey.merge(r.key() + "(tombstone)", 1, Integer::sum);
            } else if (run.mine(r.value())) {
                perKey.merge(r.key(), 1, Integer::sum);
            }
        }
        System.out.printf("%s: tổng %d record; theo key: %s%n", label, recs.size(), perKey);
    }
}
