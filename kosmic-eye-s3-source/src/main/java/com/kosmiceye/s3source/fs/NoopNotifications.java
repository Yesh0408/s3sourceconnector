package com.kosmiceye.s3source.fs;
import com.kosmiceye.s3source.spi.NotificationQueue;
public final class NoopNotifications implements NotificationQueue {
  @Override public Event poll(long t){ return null; }
  @Override public void ack(Event e){}
}
