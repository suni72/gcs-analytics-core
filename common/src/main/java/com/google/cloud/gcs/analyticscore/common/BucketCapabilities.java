package com.google.cloud.gcs.analyticscore.common;

public class BucketCapabilities {
  private final boolean hnsEnabled;

  public BucketCapabilities(boolean hnsEnabled) {
    this.hnsEnabled = hnsEnabled;
  }

  public boolean isHnsEnabled() {
    return hnsEnabled;
  }
}
