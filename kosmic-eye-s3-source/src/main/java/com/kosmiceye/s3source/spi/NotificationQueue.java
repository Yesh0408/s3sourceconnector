package com.kosmiceye.s3source.spi;
public interface NotificationQueue {
  record Event(String bucket, String key, String etag, long size, long lastModifiedMillis) {}
  Event poll(long timeoutMillis);
  void ack(Event e);
}
