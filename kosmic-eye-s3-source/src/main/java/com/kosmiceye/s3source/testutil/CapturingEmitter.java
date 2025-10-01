package com.kosmiceye.s3source.testutil;
import com.kosmiceye.s3source.spi.RecordEmitter;
import java.util.*;
public final class CapturingEmitter implements RecordEmitter {
  public static record Rec(String topic, Map<String,String> h, byte[] key, byte[] value) {}
  public final List<Rec> out = new ArrayList<>();
  public final List<Rec> dlq = new ArrayList<>();
  @Override public void emit(String topic, Map<String,String> h, byte[] key, byte[] value, long ts){ out.add(new Rec(topic,h,key,value)); }
  @Override public void emitDlq(String dlqTopic, Map<String,String> h, byte[] original, String errorClass, String errorMsg){ dlq.add(new Rec(dlqTopic,h,null,original)); }
}
