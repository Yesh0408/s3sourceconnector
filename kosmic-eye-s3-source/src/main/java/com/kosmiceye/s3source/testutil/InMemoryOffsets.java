package com.kosmiceye.s3source.testutil;
import com.kosmiceye.s3source.spi.OffsetRepository;
import java.util.*;
public final class InMemoryOffsets implements OffsetRepository {
  private final Map<String,Map<String,Object>> m = new HashMap<>();
  @Override public Map<String,Object> load(Partition p){ return m.get(key(p)); }
  @Override public void store(Partition p, Map<String,Object> off){ m.put(key(p), new HashMap<>(off)); }
  private static String key(Partition p){ return p.bucket()+"|"+p.key()+"|"+p.etag(); }
}
