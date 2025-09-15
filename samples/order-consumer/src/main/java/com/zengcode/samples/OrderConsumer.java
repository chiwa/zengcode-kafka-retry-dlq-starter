package com.zengcode.samples;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {
  @KafkaListener(topics = "orders")
  public void handle(String message) {
    if (message.contains("FAIL")) {
      throw new RuntimeException("boom for retry/dlq!");
    }
    System.out.println("Processed: " + message);
  }
}
