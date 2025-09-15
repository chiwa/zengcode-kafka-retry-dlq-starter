package com.zengcode.kafka.retry;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("kafka.retry")
public class KafkaRetryProperties {
  public enum BackoffType { FIXED, EXPONENTIAL }

  private boolean enabled = true;
  private BackoffType backoffType = BackoffType.EXPONENTIAL;
  private long initialIntervalMs = 1000;
  private double multiplier = 2.0;
  private long maxIntervalMs = 30000;
  private int maxAttempts = 5;
  private String retrySuffix = ".retry";
  private String dlqSuffix = ".dlq";
  private boolean autoCreateTopics = true;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public BackoffType getBackoffType() {
    return backoffType;
  }

  public void setBackoffType(BackoffType backoffType) {
    this.backoffType = backoffType;
  }

  public long getInitialIntervalMs() {
    return initialIntervalMs;
  }

  public void setInitialIntervalMs(long initialIntervalMs) {
    this.initialIntervalMs = initialIntervalMs;
  }

  public double getMultiplier() {
    return multiplier;
  }

  public void setMultiplier(double multiplier) {
    this.multiplier = multiplier;
  }

  public long getMaxIntervalMs() {
    return maxIntervalMs;
  }

  public void setMaxIntervalMs(long maxIntervalMs) {
    this.maxIntervalMs = maxIntervalMs;
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public void setMaxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
  }

  public String getRetrySuffix() {
    return retrySuffix;
  }

  public void setRetrySuffix(String retrySuffix) {
    this.retrySuffix = retrySuffix;
  }

  public String getDlqSuffix() {
    return dlqSuffix;
  }

  public void setDlqSuffix(String dlqSuffix) {
    this.dlqSuffix = dlqSuffix;
  }

  public boolean isAutoCreateTopics() {
    return autoCreateTopics;
  }

  public void setAutoCreateTopics(boolean autoCreateTopics) {
    this.autoCreateTopics = autoCreateTopics;
  }
}
