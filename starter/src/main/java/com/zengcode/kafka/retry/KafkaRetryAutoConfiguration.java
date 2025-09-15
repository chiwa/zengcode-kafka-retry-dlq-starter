package com.zengcode.kafka.retry;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.util.backoff.FixedBackOff;

@AutoConfiguration
@EnableConfigurationProperties(KafkaRetryProperties.class)
@ConditionalOnProperty(prefix = "kafka.retry", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaRetryAutoConfiguration {

  private static final String RETRY_ATTEMPT_HEADER = "x-retry-attempt";

  @Bean
  DefaultErrorHandler kafkaRetryDefaultErrorHandler(KafkaTemplate<byte[], byte[]> template,
                                                    KafkaRetryProperties props) {
    var recoverer = new DeadLetterPublishingRecoverer(template, (rec, ex) -> {
      String topic = rec.topic();
      int attempt = extractAttempt(rec); // 0 ถ้าไม่มี header

      String retrySuffix = props.getRetrySuffix();
      String dlqSuffix = props.getDlqSuffix();

      // base topic (ตัด retrySuffix ถ้ามี)
      String base = topic.contains(retrySuffix)
              ? topic.substring(0, topic.indexOf(retrySuffix))
              : topic;

      if (attempt + 1 >= props.getMaxAttempts()) {
        // หมดโควตา → DLQ
        return new TopicPartition(base + dlqSuffix, rec.partition());
      } else {
        // ยังเหลือโควตา → ไป retry
        return new TopicPartition(base + retrySuffix, rec.partition());
      }
    });

    var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0));
    // ✅ ให้คอมมิต/แอ็คแน่นอนหลัง recover เพื่อกันวน/หาย
    handler.setCommitRecovered(true);
    handler.setAckAfterHandle(true);
    return handler;
  }

  private static int extractAttempt(ConsumerRecord<?, ?> rec) {
    var h = rec.headers().lastHeader(RETRY_ATTEMPT_HEADER);
    if (h == null) return 0;
    try {
      return Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Bean
  RetryBridgeListener retryBridgeListener(KafkaTemplate<byte[], byte[]> template, KafkaRetryProperties props) {
    return new RetryBridgeListener(template, props);
  }

  static final class RetryBridgeListener {
    private final KafkaTemplate<byte[], byte[]> template;
    private final KafkaRetryProperties props;

    RetryBridgeListener(KafkaTemplate<byte[], byte[]> template, KafkaRetryProperties props) {
      this.template = template;
      this.props = props;
    }

    @KafkaListener(
            topicPattern = ".*\\.retry$",
            groupId = "${spring.application.name:app}-retry-bridge"
    )
    public void onRetry(ConsumerRecord<byte[], byte[]> rec,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

      String retrySuffix = props.getRetrySuffix();
      int idx = topic.indexOf(retrySuffix);
      String base = (idx > 0) ? topic.substring(0, idx) : topic; // กัน NPE/ค่าประหลาด

      // อ่าน attempt เดิม (+1)
      int prevAttempt = extractAttempt(rec);
      int nextAttempt = prevAttempt + 1;

      // ทำสำเนา headers + อัปเดต attempt
      RecordHeaders headers = new RecordHeaders();
      rec.headers().forEach(h -> {
        if (!RETRY_ATTEMPT_HEADER.equals(h.key())) {
          headers.add(h);
        }
      });
      headers.add(RETRY_ATTEMPT_HEADER, Integer.toString(nextAttempt).getBytes(StandardCharsets.UTF_8));

      // ดีเลย์ตาม initialIntervalMs
      try {
        Thread.sleep(props.getInitialIntervalMs());
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }

      ProducerRecord<byte[], byte[]> out =
              new ProducerRecord<>(base, rec.partition(), null, rec.key(), rec.value(), headers);

      template.send(out);
    }
  }

  @Bean
  public static BeanPostProcessor kafkaFactoryErrorHandlerPostProcessor(CommonErrorHandler handler) {
    return new BeanPostProcessor() {
      @Override
      public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (bean instanceof ConcurrentKafkaListenerContainerFactory<?, ?> f) {
          @SuppressWarnings("unchecked")
          var fac = (ConcurrentKafkaListenerContainerFactory<Object, Object>) f;
          fac.setCommonErrorHandler(handler);
        }
        return bean;
      }
    };
  }
}