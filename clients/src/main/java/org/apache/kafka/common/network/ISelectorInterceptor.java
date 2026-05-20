package org.apache.kafka.common.network;

public interface ISelectorInterceptor {
  void beforePutCompletedReceive(KafkaChannel channel, long currentTimeMs);
}
