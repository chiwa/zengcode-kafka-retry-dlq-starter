package com.zengcode.kafka.retry;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;

public class RetryingRecordInterceptor implements RecordInterceptor<byte[], byte[]> {

  private final KafkaRetryErrorHandler handler;

  public RetryingRecordInterceptor(KafkaRetryErrorHandler handler) {
    this.handler = handler;
  }

  @Override
  public ConsumerRecord<byte[], byte[]> intercept(
          ConsumerRecord<byte[], byte[]> record,
          Consumer<byte[], byte[]> consumer) {
    // pass-through ก่อน
    return record;
  }

  @Override
  public void failure(ConsumerRecord<byte[], byte[]> record, Exception ex,
                      Consumer<byte[], byte[]> consumer) {
    if (ex != null) {
      handler.handle(ex, record);
    }
  }
}