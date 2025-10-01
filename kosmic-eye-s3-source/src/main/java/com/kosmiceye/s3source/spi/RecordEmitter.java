package com.kosmiceye.s3source.spi;
import java.util.Map;
public interface RecordEmitter {
  void emit(String topic, Map<String,String> headers, byte[] key, byte[] value, long timestampMillis);
  void emitDlq(String dlqTopic, Map<String,String> headers, byte[] original, String errorClass, String errorMsg);
}
