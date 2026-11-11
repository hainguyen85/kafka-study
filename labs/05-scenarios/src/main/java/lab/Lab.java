package lab;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Tiện ích dùng chung cho các tình huống. */
final class Lab {
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Lab() {
    }

    static void say(String fmt, Object... args) {
        System.out.println("[" + LocalTime.now().format(TS) + "] " + String.format(fmt, args));
    }

    static void append(String file, String line) {
        try {
            Path p = Path.of("out", file);
            Files.createDirectories(p.getParent());
            Files.writeString(p, line + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static List<String> read(String file) {
        try {
            Path p = Path.of("out", file);
            return Files.exists(p) ? Files.readAllLines(p) : List.of();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void delete(String file) {
        try {
            Files.deleteIfExists(Path.of("out", file));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Một record đọc ra từ topic (value có thể null: tombstone). */
    record Rec(int partition, long offset, String key, String value) {
    }

    /**
     * Đọc toàn bộ topic từ đầu bằng KafkaConsumer thuần (không group, không commit).
     * isolation: "read_committed" hoặc "read_uncommitted". maxPartitionFetchBytes: null = mặc định.
     */
    static List<Rec> readAll(String bootstrap, String topic, String isolation, Integer maxPartitionFetchBytes) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        if (maxPartitionFetchBytes != null) {
            p.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        }
        List<Rec> out = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = c.partitionsFor(topic).stream()
                    .map(pi -> new TopicPartition(topic, pi.partition())).toList();
            c.assign(tps);
            c.seekToBeginning(tps);
            var end = c.endOffsets(tps);
            int quiet = 0;
            // Với read_committed, endOffsets có thể lớn hơn offset cuối đọc được (marker giao dịch), nên dừng khi yên lặng.
            while (quiet < 3 && tps.stream().anyMatch(tp -> c.position(tp) < end.get(tp))) {
                var recs = c.poll(Duration.ofSeconds(1));
                if (recs.isEmpty()) {
                    quiet++;
                } else {
                    quiet = 0;
                }
                for (ConsumerRecord<String, String> r : recs) {
                    out.add(new Rec(r.partition(), r.offset(), r.key(), r.value()));
                }
            }
        }
        return out;
    }
}
