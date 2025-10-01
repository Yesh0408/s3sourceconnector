package com.kosmiceye.s3source.core;
import java.util.*;
public final class S3SourceConfig {
  public final String bucket;
  public final List<String> prefixes;
  public final String region;
  public final String topic;
  public final String dlqTopic;
  public final String discoverMode;
  public final int listPageSize;
  public final long listIntervalMs;
  public final String readerFormat;
  public final String compression;
  public final int batchMaxRecords;
  public final long maxBytesPerPoll;
  public final int offsetFlushRecords;
  public final long offsetFlushIntervalMs;
  public S3SourceConfig(Map<String,String> m) {
    bucket = req(m,"s3.bucket.name");
    prefixes = List.of(m.getOrDefault("s3.prefixes","").split(",")).stream().map(String::trim).filter(s->!s.isEmpty()).toList();
    region = m.getOrDefault("s3.region","us-west-2");
    topic = req(m,"topic");
    dlqTopic = m.getOrDefault("errors.deadletterqueue.topic.name","");
    discoverMode = m.getOrDefault("s3.discover.mode","list");
    listPageSize = parseInt(m, "s3.list.page.size", 1000, 1, 5000);
    listIntervalMs = parseLong(m, "s3.list.interval.ms", 15000, 1000, 600000);
    readerFormat = m.getOrDefault("s3.reader.format","ndjson");
    compression = m.getOrDefault("s3.reader.compression","auto");
    batchMaxRecords = parseInt(m, "batch.max.records", 1000, 1, 10000);
    maxBytesPerPoll = parseLong(m, "max.bytes.per.poll", 4000000, 1000, 100000000);
    offsetFlushRecords = parseInt(m, "offset.flush.records", 500, 1, 10000);
    offsetFlushIntervalMs = parseLong(m, "offset.flush.interval.ms", 3000, 100, 600000);
  }
  private static String req(Map<String,String> m, String k){ var v=m.get(k); if(v==null||v.isBlank()) throw new IllegalArgumentException("Missing "+k); return v; }
  private static int parseInt(Map<String,String> m,String k,int d,int min,int max){ var v=m.get(k); int x=(v==null||v.isBlank())?d:Integer.parseInt(v); if(x<min||x>max) throw new IllegalArgumentException(k); return x; }
  private static long parseLong(Map<String,String> m,String k,long d,long min,long max){ var v=m.get(k); long x=(v==null||v.isBlank())?d:Long.parseLong(v); if(x<min||x>max) throw new IllegalArgumentException(k); return x; }
}
