package com.zengcode.kafka.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(
        classes = KafkaRetryDlqWithTestcontainersIT.TestApp.class,
        properties = {
                "spring.kafka.consumer.group-id=it-consumer",
                "spring.kafka.consumer.auto-offset-reset=earliest",
                "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
                "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
                "spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer",
                "spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer",

                "kafka.retry.enabled=true",
                "kafka.retry.backoff-type=EXPONENTIAL",
                "kafka.retry.initial-interval-ms=1000",
                "kafka.retry.multiplier=2.0",
                "kafka.retry.max-interval-ms=30000",
                "kafka.retry.max-attempts=3",
                "kafka.retry.retry-suffix=.retry",
                "kafka.retry.dlq-suffix=.dlq",

                "spring.testcontainers.beans=true",
                "spring.kafka.admin.auto-create=true",

                "logging.level.org.springframework.kafka=DEBUG",
                "logging.level.com.zengcode.kafka=DEBUG"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(KafkaRetryDlqWithTestcontainersIT.Containers.class)
class KafkaRetryDlqWithTestcontainersIT {

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        KafkaContainer kafkaContainer() {
            return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
                    .withEnv("KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE", "false");
        }
    }

    @EnableKafka
    @SpringBootApplication
    @ImportAutoConfiguration(KafkaRetryAutoConfiguration.class)
    static class TestApp {
        static final BlockingQueue<String> processed = new LinkedBlockingQueue<>();
        static final BlockingQueue<String> dlq = new LinkedBlockingQueue<>();
        static final BlockingQueue<Integer> dlqAttempts = new LinkedBlockingQueue<>();

        @Bean org.apache.kafka.clients.admin.NewTopic ordersTopic() {
            return TopicBuilder.name("orders").partitions(1).replicas(1).build();
        }
        @Bean org.apache.kafka.clients.admin.NewTopic ordersRetryTopic() {
            return TopicBuilder.name("orders.retry").partitions(1).replicas(1).build();
        }
        @Bean org.apache.kafka.clients.admin.NewTopic ordersDlqTopic() {
            return TopicBuilder.name("orders.dlq").partitions(1).replicas(1).build();
        }

        @KafkaListener(topics = "orders", groupId = "it-consumer")
        void mainListener(byte[] msg) {
            String s = new String(msg, StandardCharsets.UTF_8);
            if (s.contains("FAIL")) throw new RuntimeException("boom-main");
            processed.add("main:" + s);
        }

        @KafkaListener(topics = "orders.dlq", groupId = "it-consumer")
        void dlqListener(byte[] msg,
                         // อ่าน header เป็น byte[] แล้วแปลงเองให้นิ่ง
                         @Header(name = "x-retry-attempt", required = false) byte[] attemptHeader) {
            String s = new String(msg, StandardCharsets.UTF_8);
            dlq.add(s);

            Integer attempt = 0;
            if (attemptHeader != null) {
                try {
                    attempt = Integer.parseInt(new String(attemptHeader, StandardCharsets.UTF_8));
                } catch (NumberFormatException ignored) { /* keep 0 */ }
            }
            dlqAttempts.add(attempt);
        }

        @Bean BlockingQueue<String> processedQueue() { return processed; }
        @Bean BlockingQueue<String> dlqQueue() { return dlq; }
        @Bean BlockingQueue<Integer> dlqAttemptsQueue() { return dlqAttempts; }
    }

    @Autowired KafkaTemplate<byte[], byte[]> template;

    @Test
    void fail_on_main_then_retry_then_dlq() {
        template.send(new ProducerRecord<>("orders", "FAIL-CASE-1".getBytes(StandardCharsets.UTF_8)));
        template.flush();

        // รอข้อความโผล่ใน DLQ แบบ “ดึงออก” จากคิว (กัน false negative จาก timing)
        String dlqMsg =
                await().atMost(Duration.ofSeconds(40))
                        .until(() -> TestApp.dlq.poll(200, TimeUnit.MILLISECONDS), Objects::nonNull);
        assertThat(dlqMsg).isEqualTo("FAIL-CASE-1");

        // รอ header attempt ให้แน่ใจว่ามาแล้ว และเท่ากับ 1
        Integer attempt =
                await().atMost(Duration.ofSeconds(5))
                        .until(() -> TestApp.dlqAttempts.poll(200, TimeUnit.MILLISECONDS), Objects::nonNull);
        assertThat(attempt).isEqualTo(2);
    }

    @Test
    void success_on_main_never_go_to_retry_or_dlq() throws Exception {
        template.send("orders", "OK-CASE-1".getBytes(StandardCharsets.UTF_8));
        template.flush();

        String ok = TestApp.processed.poll(10, TimeUnit.SECONDS);
        assertThat(ok).isEqualTo("main:OK-CASE-1");

        String dlqMsg = TestApp.dlq.poll(2, TimeUnit.SECONDS);
        assertThat(dlqMsg).isNull();
    }
}