package com.kosmiceye.s3source.planner;
public record WorkItem(String bucket, String key, String etag, long size, long lastModifiedMillis) {}
