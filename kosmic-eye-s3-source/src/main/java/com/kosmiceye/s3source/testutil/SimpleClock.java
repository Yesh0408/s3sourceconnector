package com.kosmiceye.s3source.testutil;
import com.kosmiceye.s3source.spi.Clock;
public final class SimpleClock implements Clock {
  private long now;
  public SimpleClock(long start){ this.now=start; }
  @Override public long nowMillis(){ return now; }
  public void advance(long ms){ now+=ms; }
}
