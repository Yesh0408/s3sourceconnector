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
