echo Run: bash build_kosmic_eye_s3_connect.sh
set -e
project_dir="kosmic-eye-s3-source-connect"
mkdir -p "$project_dir/src/main/java/com/kosmiceye/s3source/spi"
mkdir -p "$project_dir/src/main/java/com/kosmiceye/s3source/core"
mkdir -p "$project_dir/src/main/java/com/kosmiceye/s3source/reader"
mkdir -p "$project_dir/src/main/java/com/kosmiceye/s3source/planner"
mkdir -p "$project_dir/src/main/java/com/kosmiceye/s3source/engine"
mkdir -p "$project_dir/src/main/java/com/kosmiceye/s3source/aws"
mkdir -p "$project_dir/src/main/java/com/kosmiceye/s3source/connect"
cat > "$project_dir/pom.xml" <<'POM'
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.kosmiceye</groupId>
  <artifactId>kosmic-eye-s3-source-connect</artifactId>
  <version>0.1.0</version>
  <properties>
    <maven.compiler.source>21</maven.compiler.source>
    <maven.compiler.target>21</maven.compiler.target>
  </properties>
  <dependencies>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>connect-api</artifactId>
      <version>3.7.0</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>software.amazon.awssdk</groupId>
      <artifactId>s3</artifactId>
      <version>2.25.64</version>
    </dependency>
  </dependencies>
</project>
POM
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/spi/ObjectStore.java" <<'EOF1'
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
EOF1
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/spi/NotificationQueue.java" <<'EOF2'
package com.kosmiceye.s3source.spi;
public interface NotificationQueue {
  record Event(String bucket, String key, String etag, long size, long lastModifiedMillis) {}
  Event poll(long timeoutMillis);
  void ack(Event e);
}
EOF2
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/spi/OffsetRepository.java" <<'EOF3'
package com.kosmiceye.s3source.spi;
import java.util.Map;
public interface OffsetRepository {
  record Partition(String bucket, String key, String etag) {}
  Map<String,Object> load(Partition p);
  void store(Partition p, Map<String,Object> offset);
}
EOF3
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/spi/RecordEmitter.java" <<'EOF4'
package com.kosmiceye.s3source.spi;
import java.util.Map;
public interface RecordEmitter {
  void emit(String topic, Map<String,String> headers, byte[] key, byte[] value, long timestampMillis);
  void emitDlq(String dlqTopic, Map<String,String> headers, byte[] original, String errorClass, String errorMsg);
}
EOF4
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/spi/Clock.java" <<'EOF5'
package com.kosmiceye.s3source.spi;
public interface Clock { long nowMillis(); }
EOF5
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/core/S3SourceConfig.java" <<'EOF6'
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
  public final int batchMaxRecords;
  public S3SourceConfig(Map<String,String> m) {
    bucket = req(m,"s3.bucket.name");
    prefixes = List.of(m.getOrDefault("s3.prefixes","").split(",")).stream().map(String::trim).filter(s->!s.isEmpty()).toList();
    region = m.getOrDefault("s3.region","us-west-2");
    topic = req(m,"topic");
    dlqTopic = m.getOrDefault("errors.deadletterqueue.topic.name","");
    discoverMode = m.getOrDefault("s3.discover.mode","list");
    listPageSize = parseInt(m, "s3.list.page.size", 1000, 1, 5000);
    listIntervalMs = parseLong(m, "s3.list.interval.ms", 15000, 1000, 600000);
    batchMaxRecords = parseInt(m, "batch.max.records", 1000, 1, 10000);
  }
  private static String req(Map<String,String> m, String k){ var v=m.get(k); if(v==null||v.isBlank()) throw new IllegalArgumentException("Missing "+k); return v; }
  private static int parseInt(Map<String,String> m,String k,int d,int min,int max){ var v=m.get(k); int x=(v==null||v.isBlank())?d:Integer.parseInt(v); if(x<min||x>max) throw new IllegalArgumentException(k); return x; }
  private static long parseLong(Map<String,String> m,String k,long d,long min,long max){ var v=m.get(k); long x=(v==null||v.isBlank())?d:Long.parseLong(v); if(x<min||x>max) throw new IllegalArgumentException(k); return x; }
}
EOF6
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/reader/InputSourceReader.java" <<'EOF7'
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
EOF7
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/reader/NdjsonReader.java" <<'EOF8'
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
EOF8
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/planner/WorkItem.java" <<'EOF9'
package com.kosmiceye.s3source.planner;
public record WorkItem(String bucket, String key, String etag, long size, long lastModifiedMillis) {}
EOF9
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/planner/WorkPlanner.java" <<'EOF10'
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
EOF10
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/engine/SourceEngine.java" <<'EOF11'
package com.kosmiceye.s3source.engine;
import com.kosmiceye.s3source.core.S3SourceConfig;
import com.kosmiceye.s3source.spi.*;
import com.kosmiceye.s3source.reader.*;
import com.kosmiceye.s3source.planner.*;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.util.*;
import java.util.zip.GZIPInputStream;
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
    boolean isGzip = detectGzip(w.key());
    long startByte = isGzip ? 0L : progress.bytePos();
    try(InputStream base = store.open(new ObjectStore.ObjectId(w.bucket(), w.key()), startByte);
        InputStream in = wrapCompression(base, isGzip)){
      InputSourceReader reader = new NdjsonReader();
      var resumeAt = isGzip ? new InputSourceReader.Progress(-1, 0) : progress;
      reader.open(in, resumeAt);
      if(isGzip && progress.recordIndex() >= 0){
        long toSkip = progress.recordIndex() + 1;
        long skipped = 0;
        while(skipped < toSkip && reader.hasNext()){
          reader.nextValueBytes();
          skipped++;
        }
      }
      int emitted=0;
      while(emitted < cfg.batchMaxRecords && reader.hasNext()){
        byte[] value = reader.nextValueBytes();
        Map<String,String> headers = Map.of("s3.bucket", w.bucket(), "s3.key", w.key(), "s3.etag", w.etag());
        emitter.emit(cfg.topic, headers, null, value, clock.nowMillis());
        emitted++;
        var p = reader.currentProgress();
        long bytePosForOffset = isGzip ? 0L : p.bytePos();
        offsets.store(partition, Map.of("index", p.recordIndex(), "bytePos", bytePosForOffset, "completed", false));
      }
      if(reader.isComplete()){
        var p = reader.currentProgress();
        long bytePosForOffset = isGzip ? 0L : p.bytePos();
        offsets.store(partition, Map.of("index", p.recordIndex(), "bytePos", bytePosForOffset, "completed", true));
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
    if(off==null) return new InputSourceReader.Progress(-1, 0);
    long idx = ((Number)off.getOrDefault("index",-1)).longValue();
    long pos = ((Number)off.getOrDefault("bytePos",0)).longValue();
    return new InputSourceReader.Progress(idx, pos);
  }
  private static boolean detectGzip(String key){ return key != null && key.toLowerCase().endsWith(".gz"); }
  private static InputStream wrapCompression(InputStream in, boolean gzip) throws Exception{
    InputStream s = new BufferedInputStream(in, 65536);
    return gzip ? new GZIPInputStream(s, 65536) : s;
  }
}
EOF11
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/aws/AwsS3Store.java" <<'EOF12'
package com.kosmiceye.s3source.aws;
import com.kosmiceye.s3source.spi.ObjectStore;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.regions.Region;
import java.io.*;
import java.util.*;
public final class AwsS3Store implements ObjectStore {
  private final S3Client s3;
  public AwsS3Store(String region){
    this.s3 = S3Client.builder().region(Region.of(region)).build();
  }
  @Override public ListPage list(String bucket, String prefix, String pageToken, int pageSize){
    List<ObjectMeta> items = new ArrayList<>();
    ListObjectsV2Request.Builder b = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(pageSize);
    if(pageToken!=null && !pageToken.isEmpty()) b.continuationToken(pageToken);
    var out = s3.listObjectsV2(b.build());
    for(S3Object o: out.contents()){
      items.add(new ObjectMeta(bucket, o.key(), o.eTag(), o.size(), o.lastModified().toEpochMilli()));
    }
    String next = out.isTruncated() ? out.nextContinuationToken() : null;
    return new ListPage(items, next);
  }
  @Override public InputStream open(ObjectId id, long startByte){
    GetObjectRequest req = startByte>0
      ? GetObjectRequest.builder().bucket(id.bucket()).key(id.key()).range("bytes="+startByte+"-").build()
      : GetObjectRequest.builder().bucket(id.bucket()).key(id.key()).build();
    return s3.getObject(req);
  }
}
EOF12
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/connect/KosmicS3SourceConnector.java" <<'EOF13'
package com.kosmiceye.s3source.connect;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.common.config.ConfigDef;
import java.util.*;
public final class KosmicS3SourceConnector extends SourceConnector {
  private Map<String,String> config;
  @Override public String version(){ return "0.1.0"; }
  @Override public void start(Map<String,String> props){ this.config = new HashMap<>(props); }
  @Override public Class<? extends SourceTask> taskClass(){ return KosmicS3SourceTask.class; }
  @Override public List<Map<String,String>> taskConfigs(int maxTasks){
    int n = Math.max(1, maxTasks);
    List<Map<String,String>> l = new ArrayList<>(n);
    for(int i=0;i<n;i++) l.add(new HashMap<>(config));
    return l;
  }
  @Override public void stop(){}
  @Override public ConfigDef config(){
    return new ConfigDef()
      .define("s3.bucket.name", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Bucket")
      .define("s3.prefixes", ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Comma-separated prefixes")
      .define("s3.region", ConfigDef.Type.STRING, "us-west-2", ConfigDef.Importance.MEDIUM, "Region")
      .define("topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target topic")
      .define("s3.discover.mode", ConfigDef.Type.STRING, "list", ConfigDef.Importance.MEDIUM, "Discovery mode")
      .define("s3.list.page.size", ConfigDef.Type.INT, 1000, ConfigDef.Importance.MEDIUM, "List page size")
      .define("s3.list.interval.ms", ConfigDef.Type.LONG, 15000L, ConfigDef.Importance.MEDIUM, "List interval")
      .define("batch.max.records", ConfigDef.Type.INT, 1000, ConfigDef.Importance.MEDIUM, "Max records per poll")
      .define("errors.deadletterqueue.topic.name", ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "DLQ topic");
  }
}
EOF13
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/connect/KosmicS3SourceTask.java" <<'EOF14'
package com.kosmiceye.s3source.connect;
import org.apache.kafka.connect.source.*;
import java.util.*;
import com.kosmiceye.s3source.core.S3SourceConfig;
import com.kosmiceye.s3source.engine.SourceEngine;
import com.kosmiceye.s3source.planner.WorkPlanner;
import com.kosmiceye.s3source.spi.*;
import com.kosmiceye.s3source.aws.AwsS3Store;
public final class KosmicS3SourceTask extends SourceTask {
  private SourceEngine engine;
  private ConnectEmitter emitter;
  private ConnectOffsets offsets;
  @Override public String version(){ return "0.1.0"; }
  @Override public void start(Map<String,String> props){
    var cfg = new S3SourceConfig(props);
    ObjectStore store = new AwsS3Store(cfg.region);
    NotificationQueue nq = null;
    Clock clock = new ConnectClock();
    offsets = new ConnectOffsets(context);
    emitter = new ConnectEmitter(context, cfg.topic);
    var planner = new WorkPlanner(cfg, store, nq, clock);
    engine = new SourceEngine(cfg, store, offsets, emitter, planner, clock);
  }
  @Override public List<SourceRecord> poll() throws InterruptedException {
    emitter.begin();
    int polled = engine.pollOnce();
    return emitter.end();
  }
  @Override public void stop(){}
}
EOF14
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/connect/ConnectOffsets.java" <<'EOF15'
package com.kosmiceye.s3source.connect;
import com.kosmiceye.s3source.spi.OffsetRepository;
import org.apache.kafka.connect.source.SourceTaskContext;
import java.util.*;
public final class ConnectOffsets implements OffsetRepository {
  private final SourceTaskContext ctx;
  public ConnectOffsets(SourceTaskContext ctx){ this.ctx = ctx; }
  @Override public Map<String,Object> load(Partition p){
    Map<String,String> part = Map.of("bucket", p.bucket(), "key", p.key(), "etag", p.etag());
    return ctx.offsetStorageReader().offset(part);
  }
  @Override public void store(Partition p, Map<String,Object> offset){
    Map<String,String> part = Map.of("bucket", p.bucket(), "key", p.key(), "etag", p.etag());
    ctx.offsetStorageWriter().offset(part, offset);
    ctx.offsetStorageWriter().flush();
  }
}
EOF15
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/connect/ConnectEmitter.java" <<'EOF16'
package com.kosmiceye.s3source.connect;
import com.kosmiceye.s3source.spi.RecordEmitter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import java.util.*;
public final class ConnectEmitter implements RecordEmitter {
  private final SourceTaskContext ctx;
  private final String topic;
  private final List<SourceRecord> buf = new ArrayList<>();
  public ConnectEmitter(SourceTaskContext ctx, String topic){ this.ctx=ctx; this.topic=topic; }
  public void begin(){ buf.clear(); }
  public List<SourceRecord> end(){ return new ArrayList<>(buf); }
  @Override public void emit(String t, Map<String,String> headers, byte[] key, byte[] value, long ts){
    Map<String,String> part = Map.of("emitter","kosmic");
    Map<String,Object> off = Map.of();
    SourceRecord rec = new SourceRecord(part, off, topic, null, null, null, value);
    headers.forEach((k,v)-> rec.headers().addString(k, v));
    buf.add(rec);
  }
  @Override public void emitDlq(String dlqTopic, Map<String,String> headers, byte[] original, String errorClass, String errorMsg){
  }
}
EOF16
cat > "$project_dir/src/main/java/com/kosmiceye/s3source/connect/ConnectClock.java" <<'EOF17'
package com.kosmiceye.s3source.connect;
import com.kosmiceye.s3source.spi.Clock;
public final class ConnectClock implements Clock { @Override public long nowMillis(){ return System.currentTimeMillis(); } }
EOF17
pushd "$project_dir" >/dev/null
mvn -q -DskipTests package
jar_path="$(pwd)/target/kosmic-eye-s3-source-connect-0.1.0.jar"
echo "$jar_path"
echo "ls $(pwd)/target"
echo "curl -s http://localhost:8083/connector-plugins | jq '.[].class'"
echo "curl -s -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d '{\"name\":\"kosmic-s3\",\"config\":{\"connector.class\":\"com.kosmiceye.s3source.connect.KosmicS3SourceConnector\",\"tasks.max\":\"1\",\"s3.bucket.name\":\"your-bucket\",\"s3.prefixes\":\"path/prefix\",\"s3.region\":\"us-west-2\",\"topic\":\"your-topic\"}}'"
popd >/dev/null
