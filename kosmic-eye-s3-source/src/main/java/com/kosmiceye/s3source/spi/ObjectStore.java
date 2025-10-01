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
