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
