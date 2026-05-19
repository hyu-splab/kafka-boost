package org.apache.kafka.common.network;

public interface ISelectorInterceptors {
  default void beforePutCompletedReceive(KafkaChannel channel, long currentTimeMs) {}
}
