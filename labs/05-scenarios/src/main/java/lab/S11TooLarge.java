package lab;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * S11: message 2 MB: lỗi ở một tầng, sửa xong lại lỗi ở tầng kế.
 *   Lần 1: producer mặc định (max.request.size=1048576) => lỗi ngay tại producer.
 *   Lần 2: producer max.request.size=3 MB, topic mặc định (max.message.bytes ~1 MB) => broker từ chối.
 *   Lần 3: producer 3 MB, topic s11-big-ok (max.message.bytes=3000000) => thành công.
 *   Lần 4: consumer mặc định (max.partition.fetch.bytes=1 MB) đọc record 2 MB: vẫn đọc được, vì giới hạn fetch
 *          không phải giới hạn cứng (batch đầu tiên quá lớn vẫn được trả; docs consumer configs).
 * Giá trị mặc định lấy từ docs 4.4 (research/0001, T11). In ra đúng loại exception bạn thấy.
 */
@Component
@Profile("s11")
public class S11TooLarge implements ApplicationRunner {
    private static final int BYTES = 2_000_000;

    private final Environment env;
    private final ConfigurableApplicationContext ctx;

    public S11TooLarge(Environment env, ConfigurableApplicationContext ctx) {
        this.env = env;
        this.ctx = ctx;
    }

    @Override
    public void run(ApplicationArguments args) {
        String bs = env.getProperty("spring.kafka.bootstrap-servers");
        attempt(bs, "lần 1: producer mặc định, topic mặc định", "s11-big-default", null);
        attempt(bs, "lần 2: producer 3MB, topic mặc định    ", "s11-big-default", 3_000_000);
        attempt(bs, "lần 3: producer 3MB, topic max.message.bytes=3000000", "s11-big-ok", 3_000_000);
        List<Lab.Rec> got = Lab.readAll(bs, "s11-big-ok", "read_uncommitted", null); // consumer mặc định
        long big = got.stream().filter(r -> r.value() != null && r.value().length() >= BYTES).count();
        Lab.say("lần 4: consumer mặc định (max.partition.fetch.bytes=1MB) đọc được %d record 2MB", big);
        ctx.close();
    }

    private static void attempt(String bs, String label, String topic, Integer maxRequestSize) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20000);
        if (maxRequestSize != null) {
            p.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        }
        try (KafkaProducer<String, String> k = new KafkaProducer<>(p)) {
            k.send(new ProducerRecord<>(topic, "k", "x".repeat(BYTES))).get(30, TimeUnit.SECONDS);
            Lab.say("%s => THÀNH CÔNG", label);
        } catch (Exception e) {
            Throwable c = e.getCause() != null ? e.getCause() : e;
            Lab.say("%s => LỖI %s: %s", label, c.getClass().getSimpleName(), c.getMessage());
        }
    }
}
