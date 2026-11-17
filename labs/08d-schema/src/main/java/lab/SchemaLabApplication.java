package lab;

import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Lab Bài 8d: gửi và đọc sự kiện PaymentCompleted bằng Avro + Confluent Schema Registry.
 *
 * Gửi:  --spring.profiles.active=produce --lab.schema=v1   (v1 | v2 | v3)
 * Đọc:  --spring.profiles.active=consume
 *
 * Producer dùng GenericRecord dựng từ file .avsc (không sinh code) để thấy rõ schema nào đang được gửi.
 * KafkaAvroSerializer tự đăng ký schema dưới subject "payments-avro-value" nếu qua được kiểm tra compatibility
 * (mặc định BACKWARD); không qua thì send() lỗi, và ứng dụng in ra lỗi đó.
 * Consumer in tên các field của schema mà message ĐƯỢC GHI bằng (writer schema): message v1 và v2 nằm chung topic.
 */
@SpringBootApplication
public class SchemaLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchemaLabApplication.class, args);
    }

    @Bean
    NewTopic paymentsAvro(@Value("${lab.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(3).config("min.insync.replicas", "2").build();
    }

    @Bean
    @Profile("produce")
    ApplicationRunner producer(KafkaTemplate<String, Object> template,
                               @Value("${lab.topic}") String topic,
                               @Value("${lab.schema}") String version,
                               @Value("${lab.count}") int count) {
        return args -> {
            Schema schema;
            try (InputStream in = SchemaLabApplication.class.getResourceAsStream("/schemas/payment-" + version + ".avsc")) {
                schema = new Schema.Parser().parse(in);
            }
            System.out.printf("[produce] schema %s, field: %s%n", version, schema.getFields().stream().map(Schema.Field::name).toList());
            for (int i = 0; i < count; i++) {
                GenericRecord rec = new GenericData.Record(schema);
                rec.put("eventId", UUID.randomUUID().toString());
                rec.put("accountId", "ACC-00" + (i % 3));
                rec.put("amount", 100_000L * (i + 1));
                rec.put("currency", "VND");
                if (schema.getField("channel") != null) rec.put("channel", "mobile");
                if (schema.getField("branchCode") != null) rec.put("branchCode", "HN01");
                try {
                    var md = template.send(topic, (String) rec.get("accountId").toString(), rec)
                            .get(30, TimeUnit.SECONDS).getRecordMetadata();
                    System.out.printf("[produce] %s -> partition=%d offset=%d%n", rec.get("accountId"), md.partition(), md.offset());
                } catch (Exception e) {
                    Throwable root = e;
                    while (root.getCause() != null) root = root.getCause();
                    System.out.printf("[produce] GỬI LỖI (%s): %s%n", root.getClass().getSimpleName(), root.getMessage());
                    return;
                }
            }
        };
    }

    @Bean
    @Profile("consume")
    Object consumer() {
        return new Object() {
            @KafkaListener(topics = "${lab.topic}")
            void onMessage(ConsumerRecord<String, GenericRecord> rec) {
                GenericRecord v = rec.value();
                if (v == null) {
                    System.out.printf("[consume] offset=%d: value null (lỗi giải mã, xem header của ErrorHandlingDeserializer)%n", rec.offset());
                    return;
                }
                System.out.printf("[consume] partition=%d offset=%d schema field=%s value=%s%n", rec.partition(), rec.offset(),
                        v.getSchema().getFields().stream().map(Schema.Field::name).toList(), v);
            }
        };
    }
}
