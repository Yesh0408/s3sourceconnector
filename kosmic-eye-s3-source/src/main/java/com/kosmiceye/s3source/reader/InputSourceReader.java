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
