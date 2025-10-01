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
