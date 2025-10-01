package com.kosmiceye.s3source.app;
import com.kosmiceye.s3source.core.S3SourceConfig;
import com.kosmiceye.s3source.engine.SourceEngine;
import com.kosmiceye.s3source.planner.WorkPlanner;
import com.kosmiceye.s3source.spi.*;
import com.kosmiceye.s3source.fs.*;
import com.kosmiceye.s3source.testutil.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
public final class SmokeTestMain {
  public static void main(String[] args){
    var root = Path.of("sample_s3");
    var store = new FsObjectStore(root);
    var cfg = new S3SourceConfig(Map.of(
      "s3.bucket.name","bucket",
      "s3.prefixes","topics/logs",
      "topic","ingested.logs",
      "s3.discover.mode","list",
      "batch.max.records","1000",
      "s3.list.page.size","100"
    ));
    Clock clock = new SimpleClock(System.currentTimeMillis());
    NotificationQueue nq = new NoopNotifications();
    var planner = new WorkPlanner(cfg, store, nq, clock);
    var offsets = new InMemoryOffsets();
    var emitter = new CapturingEmitter();
    var engine = new SourceEngine(cfg, store, offsets, emitter, planner, clock);
    int total=0;
    for(int i=0;i<20;i++){ total += engine.pollOnce(); }
    System.out.println("OK " + emitter.out.size());
    if(!emitter.out.isEmpty()){
      var s0 = new String(emitter.out.get(0).value(), StandardCharsets.UTF_8).trim();
      System.out.println("First=" + s0);
    }
  }
}
