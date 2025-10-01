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
