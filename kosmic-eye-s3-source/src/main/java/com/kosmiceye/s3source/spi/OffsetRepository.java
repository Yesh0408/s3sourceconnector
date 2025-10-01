package com.kosmiceye.s3source.spi;
import java.util.Map;
public interface OffsetRepository {
  record Partition(String bucket, String key, String etag) {}
  Map<String,Object> load(Partition p);
  void store(Partition p, Map<String,Object> offset);
}
