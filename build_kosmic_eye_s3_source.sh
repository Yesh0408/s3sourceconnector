echo Run: bash build_kosmic_eye_s3_source.sh
set -e
ROOT=kosmic-eye-s3-source
rm -rf "$ROOT"
mkdir -p "$ROOT/src/main/java/com/kosmiceye/s3source/spi"
mkdir -p "$ROOT/src/main/java/com/kosmiceye/s3source/core"
mkdir -p "$ROOT/src/main/java/com/kosmiceye/s3source/reader"
mkdir -p "$ROOT/src/main/java/com/kosmiceye/s3source/planner"
mkdir -p "$ROOT/src/main/java/com/kosmiceye/s3source/engine"
mkdir -p "$ROOT/src/main/java/com/kosmiceye/s3source/testutil"
mkdir -p "$ROOT/src/main/java/com/kosmiceye/s3source/fs"
mkdir -p "$ROOT/src/main/java/com/kosmiceye/s3source/app"
mkdir -p "$ROOT/out/classes"
mkdir -p "$ROOT/sample_s3/bucket/topics/logs/part=0"
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/spi/ObjectStore.java"
package com.kosmiceye.s3source.spi;
import java.io.InputStream;
import java.util.List;
public interface ObjectStore {
  record ObjectId(String bucket, String key) {}
  record ObjectMeta(String bucket, String key, String etag, long size, long lastModifiedMillis) {}
  record ListPage(List<ObjectMeta> items, String nextPageToken) {}
  ListPage list(String bucket, String prefix, String pageToken, int pageSize);
  InputStream open(ObjectId id, long startByte);
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/spi/NotificationQueue.java"
package com.kosmiceye.s3source.spi;
public interface NotificationQueue {
  record Event(String bucket, String key, String etag, long size, long lastModifiedMillis) {}
  Event poll(long timeoutMillis);
  void ack(Event e);
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/spi/OffsetRepository.java"
package com.kosmiceye.s3source.spi;
import java.util.Map;
public interface OffsetRepository {
  record Partition(String bucket, String key, String etag) {}
  Map<String,Object> load(Partition p);
  void store(Partition p, Map<String,Object> offset);
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/spi/RecordEmitter.java"
package com.kosmiceye.s3source.spi;
import java.util.Map;
public interface RecordEmitter {
  void emit(String topic, Map<String,String> headers, byte[] key, byte[] value, long timestampMillis);
  void emitDlq(String dlqTopic, Map<String,String> headers, byte[] original, String errorClass, String errorMsg);
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/spi/Clock.java"
package com.kosmiceye.s3source.spi;
public interface Clock { long nowMillis(); }
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/core/S3SourceConfig.java"
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
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/reader/InputSourceReader.java"
package com.kosmiceye.s3source.reader;
import java.io.InputStream;
public interface InputSourceReader {
  record Progress(long recordIndex, long bytePos) {}
  void open(InputStream in, Progress resumeFrom) throws Exception;
  boolean hasNext() throws Exception;
  byte[] nextValueBytes() throws Exception;
  Progress currentProgress();
  boolean isComplete();
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/reader/NdjsonReader.java"
package com.kosmiceye.s3source.reader;
import java.io.*;
public final class NdjsonReader implements InputSourceReader {
  private BufferedInputStream in;
  private long index= -1;
  private long bytePos= 0;
  private byte[] next;
  private boolean eof=false;
  @Override public void open(InputStream in, Progress resumeFrom) throws Exception {
    this.in = new BufferedInputStream(in, 65536);
    if(resumeFrom!=null){ this.index = resumeFrom.recordIndex()-1; this.bytePos = resumeFrom.bytePos(); skip(resumeFrom.bytePos()); }
    fetch();
  }
  private void skip(long n) throws IOException { long left=n; while(left>0){ long s=in.skip(left); if(s<=0) throw new EOFException("skip beyond EOF"); left-=s; } }
  private void fetch() throws IOException {
    if(eof){ next=null; return; }
    ByteArrayOutputStream buf=new ByteArrayOutputStream(1024);
    int b;
    while((b=in.read())!=-1){
      bytePos++;
      if(b=='\n') break;
      buf.write(b);
    }
    if(b==-1 && buf.size()==0){ eof=true; next=null; return; }
    next = buf.toByteArray();
    if(b==-1){ eof=true; }
  }
  @Override public boolean hasNext(){ return next!=null; }
  @Override public byte[] nextValueBytes(){ index++; byte[] out=next; try{ fetch(); }catch(Exception e){ throw new RuntimeException(e); } return out; }
  @Override public Progress currentProgress(){ return new Progress(index, bytePos); }
  @Override public boolean isComplete(){ return eof && next==null; }
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/planner/WorkItem.java"
package com.kosmiceye.s3source.planner;
public record WorkItem(String bucket, String key, String etag, long size, long lastModifiedMillis) {}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/planner/WorkPlanner.java"
package com.kosmiceye.s3source.planner;
import com.kosmiceye.s3source.core.S3SourceConfig;
import com.kosmiceye.s3source.spi.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
public final class WorkPlanner {
  private final S3SourceConfig cfg;
  private final ObjectStore store;
  private final NotificationQueue notifications;
  private final Clock clock;
  private final Map<String,Long> lastListedByPrefix = new HashMap<>();
  private final Queue<WorkItem> queue = new ConcurrentLinkedQueue<>();
  public WorkPlanner(S3SourceConfig cfg, ObjectStore store, NotificationQueue notifications, Clock clock){
    this.cfg=cfg; this.store=store; this.notifications=notifications; this.clock=clock;
  }
  public void tick() {
    long now = clock.nowMillis();
    if(cfg.discoverMode.contains("list")){
      var prefixes = cfg.prefixes.isEmpty()? List.of("") : cfg.prefixes;
      for(String prefix : prefixes){
        long last = lastListedByPrefix.getOrDefault(prefix, 0L);
        if(now-last >= cfg.listIntervalMs){
          listOnce(prefix);
          lastListedByPrefix.put(prefix, now);
        }
      }
    }
    if(notifications!=null){
      NotificationQueue.Event e;
      while((e=notifications.poll(0))!=null){
        queue.add(new WorkItem(e.bucket(), e.key(), e.etag(), e.size(), e.lastModifiedMillis()));
        notifications.ack(e);
      }
    }
  }
  private void listOnce(String prefix){
    String token=null;
    do{
      var page = store.list(cfg.bucket, prefix, token, cfg.listPageSize);
      for(var m: page.items()){
        queue.add(new WorkItem(m.bucket(), m.key(), m.etag(), m.size(), m.lastModifiedMillis()));
      }
      token = page.nextPageToken();
    } while(token!=null && !token.isEmpty());
  }
  public WorkItem next(){ return queue.poll(); }
  public int queued(){ return queue.size(); }
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/engine/SourceEngine.java"
package com.kosmiceye.s3source.engine;
import com.kosmiceye.s3source.core.S3SourceConfig;
import com.kosmiceye.s3source.spi.*;
import com.kosmiceye.s3source.reader.*;
import com.kosmiceye.s3source.planner.*;
import java.io.InputStream;
import java.util.*;
public final class SourceEngine {
  private final S3SourceConfig cfg;
  private final ObjectStore store;
  private final OffsetRepository offsets;
  private final RecordEmitter emitter;
  private final WorkPlanner planner;
  private final Clock clock;
  public SourceEngine(S3SourceConfig cfg, ObjectStore store, OffsetRepository offsets, RecordEmitter emitter, WorkPlanner planner, Clock clock){
    this.cfg=cfg; this.store=store; this.offsets=offsets; this.emitter=emitter; this.planner=planner; this.clock=clock;
  }
  public int pollOnce(){
    planner.tick();
    WorkItem w = planner.next();
    if(w==null) return 0;
    var partition = new OffsetRepository.Partition(w.bucket(), w.key(), w.etag());
    var saved = offsets.load(partition);
    var progress = toProgress(saved);
    try(InputStream in = store.open(new ObjectStore.ObjectId(w.bucket(), w.key()), progress.bytePos())){
      InputSourceReader reader = selectReader();
      reader.open(in, progress);
      int emitted=0;
      while(emitted < cfg.batchMaxRecords && reader.hasNext()){
        byte[] value = reader.nextValueBytes();
        Map<String,String> headers = Map.of("s3.bucket", w.bucket(), "s3.key", w.key(), "s3.etag", w.etag());
        emitter.emit(cfg.topic, headers, null, value, clock.nowMillis());
        emitted++;
        var p = reader.currentProgress();
        offsets.store(partition, Map.of("index", p.recordIndex(), "bytePos", p.bytePos(), "completed", false));
      }
      if(reader.isComplete()){
        var p = reader.currentProgress();
        offsets.store(partition, Map.of("index", p.recordIndex(), "bytePos", p.bytePos(), "completed", true));
      }
      return emitted;
    } catch(Exception ex){
      Map<String,String> h = Map.of("s3.bucket", w.bucket(), "s3.key", w.key(), "error", ex.getClass().getName());
      if(cfg.dlqTopic!=null && !cfg.dlqTopic.isBlank()){
        emitter.emitDlq(cfg.dlqTopic, h, null, ex.getClass().getName(), ex.getMessage());
      }
      return 0;
    }
  }
  private static InputSourceReader.Progress toProgress(Map<String,Object> off){
    if(off==null) return new InputSourceReader.Progress( -1, 0 );
    long idx = ((Number)off.getOrDefault("index",-1)).longValue();
    long pos = ((Number)off.getOrDefault("bytePos",0)).longValue();
    return new InputSourceReader.Progress(idx, pos);
  }
  private InputSourceReader selectReader(){ return new NdjsonReader(); }
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/testutil/InMemoryOffsets.java"
package com.kosmiceye.s3source.testutil;
import com.kosmiceye.s3source.spi.OffsetRepository;
import java.util.*;
public final class InMemoryOffsets implements OffsetRepository {
  private final Map<String,Map<String,Object>> m = new HashMap<>();
  @Override public Map<String,Object> load(Partition p){ return m.get(key(p)); }
  @Override public void store(Partition p, Map<String,Object> off){ m.put(key(p), new HashMap<>(off)); }
  private static String key(Partition p){ return p.bucket()+"|"+p.key()+"|"+p.etag(); }
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/testutil/CapturingEmitter.java"
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
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/testutil/SimpleClock.java"
package com.kosmiceye.s3source.testutil;
import com.kosmiceye.s3source.spi.Clock;
public final class SimpleClock implements Clock {
  private long now;
  public SimpleClock(long start){ this.now=start; }
  @Override public long nowMillis(){ return now; }
  public void advance(long ms){ now+=ms; }
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/fs/FsObjectStore.java"
package com.kosmiceye.s3source.fs;
import com.kosmiceye.s3source.spi.ObjectStore;
import java.io.*;
import java.nio.file.*;
import java.util.*;
public final class FsObjectStore implements ObjectStore {
  private final Path root;
  public FsObjectStore(Path root){ this.root = root; }
  @Override public ListPage list(String bucket, String prefix, String pageToken, int pageSize) {
    try {
      Path base = root.resolve(bucket).resolve(prefix==null?"":prefix);
      List<ObjectMeta> metas = new ArrayList<>();
      if(Files.exists(base)) {
        Files.walk(base).filter(Files::isRegularFile).forEach(p -> {
          try {
            String key = root.resolve(bucket).relativize(p).toString().replace(File.separatorChar,'/');
            long size = Files.size(p);
            long lm = Files.getLastModifiedTime(p).toMillis();
            String etag = Long.toHexString(size ^ lm);
            metas.add(new ObjectMeta(bucket, key, etag, size, lm));
          } catch (IOException e) { throw new UncheckedIOException(e); }
        });
      }
      Collections.sort(metas, Comparator.comparing(ObjectMeta::key));
      int start = (pageToken==null||pageToken.isEmpty())?0:Integer.parseInt(pageToken);
      int end = Math.min(metas.size(), start+pageSize);
      String next = end<metas.size()? String.valueOf(end) : null;
      return new ListPage(metas.subList(start,end), next);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  @Override public InputStream open(ObjectId id, long startByte) {
    try {
      Path p = root.resolve(id.bucket()).resolve(id.key());
      byte[] all = Files.readAllBytes(p);
      if(startByte<0 || startByte>all.length) throw new IllegalArgumentException("bad start");
      return new ByteArrayInputStream(all, (int)startByte, all.length - (int)startByte);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/fs/NoopNotifications.java"
package com.kosmiceye.s3source.fs;
import com.kosmiceye.s3source.spi.NotificationQueue;
public final class NoopNotifications implements NotificationQueue {
  @Override public Event poll(long t){ return null; }
  @Override public void ack(Event e){}
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/app/SmokeTestMain.java"
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
    for(int i=0;i<10;i++){ total += engine.pollOnce(); }
    if(emitter.out.size()!=2) throw new RuntimeException("expected 2 got "+emitter.out.size());
    var s0 = new String(emitter.out.get(0).value(), StandardCharsets.UTF_8).trim();
    var s1 = new String(emitter.out.get(1).value(), StandardCharsets.UTF_8).trim();
    if(!s0.startsWith("{") || !s1.startsWith("{")) throw new RuntimeException("not json lines");
    System.out.println("OK " + emitter.out.size());
  }
}
JAVA
cat <<'JAVA' > "$ROOT/src/main/java/com/kosmiceye/s3source/app/RunEngineMain.java"
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
JAVA
cat <<'MANIFEST' > "$ROOT/MANIFEST.MF"
Manifest-Version: 1.0
Main-Class: com.kosmiceye.s3source.app.RunEngineMain
MANIFEST
cat <<'DATA' > "$ROOT/sample_s3/bucket/topics/logs/part=0/file-0001.json"
{"x":1}
{"x":2}
DATA
(
  cd "$ROOT"
  find src/main/java -name "*.java" > sources.list
  javac -d out/classes @sources.list
  jar cfm kosmic-eye-s3-source.jar MANIFEST.MF -C out/classes .
  java -cp kosmic-eye-s3-source.jar com.kosmiceye.s3source.app.SmokeTestMain
)
echo "java -jar kosmic-eye-s3-source.jar"
echo "java -cp kosmic-eye-s3-source.jar com.kosmiceye.s3source.app.SmokeTestMain"
