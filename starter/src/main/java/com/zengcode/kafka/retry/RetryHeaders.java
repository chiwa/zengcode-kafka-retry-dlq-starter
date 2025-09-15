package com.zengcode.kafka.retry;
public final class RetryHeaders {
  public static final String MESSAGE_ID = "x-message-id";
  public static final String RETRY_ATTEMPT = "x-retry-attempt";
  public static final String FIRST_FAILURE_TS = "x-first-failure-ts";
  public static final String EXCEPTION_CLASS = "x-exception-class";
  public static final String EXCEPTION_MESSAGE = "x-exception-message";
  private RetryHeaders() {}
}
