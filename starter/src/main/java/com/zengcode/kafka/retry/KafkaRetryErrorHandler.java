package com.zengcode.kafka.retry;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;

final class KafkaRetryErrorHandler {

  private final KafkaTemplate<byte[], byte[]> template;
  private final KafkaRetryProperties props;

  KafkaRetryErrorHandler(KafkaTemplate<byte[], byte[]> template, KafkaRetryProperties props) {
    this.template = template;
    this.props = props;
  }

  void handle(Exception ex, ConsumerRecord<byte[], byte[]> record) {
    var headers = record.headers();
    var attempt = getInt(headers, RetryHeaders.RETRY_ATTEMPT, 0) + 1;
    var firstTs = getLong(headers, RetryHeaders.FIRST_FAILURE_TS, Instant.now().toEpochMilli());
    var messageId = getString(headers, RetryHeaders.MESSAGE_ID, UUID.randomUUID().toString());

    long nextDelayMs = nextDelay(attempt);
    boolean exceed = attempt >= props.getMaxAttempts();

    String targetTopic = exceed
        ? record.topic() + props.getDlqSuffix()
        : record.topic() + props.getRetrySuffix() + "." + nextDelayMs + "ms";

    record.headers().add(new RecordHeader(RetryHeaders.MESSAGE_ID, messageId.getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader(RetryHeaders.RETRY_ATTEMPT, String.valueOf(attempt).getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader(RetryHeaders.FIRST_FAILURE_TS, String.valueOf(firstTs).getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader(RetryHeaders.EXCEPTION_CLASS, ex.getClass().getName().getBytes(StandardCharsets.UTF_8)));
    if (ex.getMessage()!=null) {
      record.headers().add(new RecordHeader(RetryHeaders.EXCEPTION_MESSAGE, ex.getMessage().getBytes(StandardCharsets.UTF_8)));
    }

    template.send(targetTopic, record.key(), record.value());
  }

  private long nextDelay(int attempt) {
    if (props.getBackoffType() == KafkaRetryProperties.BackoffType.FIXED) {
      return Math.min(props.getMaxIntervalMs(), props.getInitialIntervalMs());
    }
    double val = props.getInitialIntervalMs() * Math.pow(props.getMultiplier(), attempt - 1);
    return Math.min((long) val, props.getMaxIntervalMs());
  }

  private static int getInt(org.apache.kafka.common.header.Headers h, String k, int d) {
    var v = h.lastHeader(k);
    return v==null? d : Integer.parseInt(new String(v.value(), StandardCharsets.UTF_8));
  }
  private static long getLong(org.apache.kafka.common.header.Headers h, String k, long d) {
    var v = h.lastHeader(k);
    return v==null? d : Long.parseLong(new String(v.value(), StandardCharsets.UTF_8));
  }
  private static String getString(org.apache.kafka.common.header.Headers h, String k, String d) {
    var v = h.lastHeader(k);
    return v==null? d : new String(v.value(), StandardCharsets.UTF_8);
  }
}
