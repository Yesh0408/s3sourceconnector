package com.kosmiceye.s3source.testutil;
public final class Assert {
  private Assert(){}
  public static void check(boolean condition, String message){
    if(!condition) throw new IllegalStateException(message);
  }
  public static void equals(Object expected, Object actual, String message){
    if(expected==null ? actual!=null : !expected.equals(actual)){
      throw new IllegalStateException(message + " expected=" + expected + " actual=" + actual);
    }
  }
  public static void equals(Object expected, Object actual){
    equals(expected, actual, "Values differ");
  }
  public static void notNull(Object value, String message){
    if(value==null) throw new IllegalStateException(message);
  }
}
