package lab;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Tự kiểm tra: chạy các tình huống với broker Kafka NHÚNG (1 node, KRaft) trong cùng JVM, không cần Docker.
 * Chỉ để kiểm tra logic consumer/error/transaction; KHÔNG kiểm tra được độ bền đa broker (acks, ISR, kill broker).
 *
 * Chạy:  java -cp target/classes;target/test-classes;<classpath> lab.EmbeddedHarness s8 s8dlt s10 ...
 * Tên tình huống: s6 s6fix s7 s8 s8dlt s8retry s9 s9dedup s10 s11 s12 s13
 */
public class EmbeddedHarness {

    public static void main(String[] args) throws Exception {
        EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, 1);
        broker.brokerProperty("offsets.topic.replication.factor", "1");
        broker.brokerProperty("transaction.state.log.replication.factor", "1");
        broker.brokerProperty("transaction.state.log.min.isr", "1");
        broker.afterPropertiesSet();
        String bs = broker.getBrokersAsString();
        System.out.println("### broker nhúng: " + bs);
        try {
            for (String s : args) {
                System.out.println("\n################ " + s + " ################");
                switch (s) {
                    case "s6", "s6fix" -> {
                        run(s, bs, "--lab.phase=crash", "--lab.crash=close");
                        run(s, bs, "--lab.phase=resume");
                    }
                    case "s12" -> {
                        run(s, bs, "--lab.phase=before");
                        Properties p = new Properties();
                        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
                        try (Admin admin = Admin.create(p)) {
                            admin.createPartitions(Map.of("s12-keys", NewPartitions.increaseTo(6))).all().get();
                        }
                        System.out.println("### đã tăng s12-keys lên 6 partition");
                        run(s, bs, "--lab.phase=after");
                    }
                    default -> run(s, bs);
                }
            }
        } finally {
            broker.destroy();
        }
        System.exit(0);
    }

    private static void run(String profile, String bs, String... extra) throws Exception {
        String[] base = {"--spring.profiles.active=" + profile, "--spring.kafka.bootstrap-servers=" + bs,
                "--lab.replicas=1", "--lab.min-isr=1"};
        String[] all = new String[base.length + extra.length];
        System.arraycopy(base, 0, all, 0, base.length);
        System.arraycopy(extra, 0, all, base.length, extra.length);
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(Application.class).run(all);
        long deadline = System.currentTimeMillis() + 240_000;
        while (ctx.isActive() && System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
        }
        if (ctx.isActive()) {
            System.out.println("### quá thời gian chờ, đóng cưỡng bức");
            ctx.close();
        }
    }
}
