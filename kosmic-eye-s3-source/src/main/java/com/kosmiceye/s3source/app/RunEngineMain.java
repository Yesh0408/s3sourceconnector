package com.kosmiceye.s3source.app;
import com.kosmiceye.s3source.core.S3SourceConfig;
import com.kosmiceye.s3source.engine.SourceEngine;
import com.kosmiceye.s3source.planner.WorkPlanner;
import com.kosmiceye.s3source.spi.*;
import com.kosmiceye.s3source.fs.*;
import com.kosmiceye.s3source.testutil.*;
import java.nio.file.Path;
import java.util.*;
public final class RunEngineMain {
  public static void main(String[] args){
    Map<String,String> cfgMap = new HashMap<>();
    cfgMap.put("s3.bucket.name", System.getProperty("bucket","bucket"));
    cfgMap.put("s3.prefixes", System.getProperty("prefixes","topics/logs"));
    cfgMap.put("topic", System.getProperty("topic","ingested.logs"));
    cfgMap.put("s3.discover.mode", System.getProperty("discover","list"));
    var cfg = new S3SourceConfig(cfgMap);
    var store = new FsObjectStore(Path.of(System.getProperty("root","sample_s3")));
    Clock clock = new SimpleClock(System.currentTimeMillis());
    NotificationQueue nq = new NoopNotifications();
    var planner = new WorkPlanner(cfg, store, nq, clock);
    var offsets = new InMemoryOffsets();
    var emitter = new CapturingEmitter();
    var engine = new SourceEngine(cfg, store, offsets, emitter, planner, clock);
    int polled=0;
    for(int i=0;i<20;i++){ polled += engine.pollOnce(); }
    System.out.println("Emitted " + polled + " records");
  }
}
