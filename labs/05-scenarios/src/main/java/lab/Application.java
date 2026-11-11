package lab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Lab 05: các tình huống "oái oăm". Chọn tình huống bằng profile: s6, s6fix, s7, s8, s8dlt, s9, s9dedup, s10, s11, s12, s13.
 * Xem README.md để biết từng tình huống làm gì và cách đọc kết quả.
 */
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
