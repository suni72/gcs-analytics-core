package com.google.cloud.gcs.analyticscore.client.namespace;

import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.common.PathType;
import java.io.IOException;

public interface NamespaceStrategy {
  GcsItemInfo getFileInfo(GcsItemId id, PathType pathType) throws IOException;

  void mkdirs(GcsItemId id) throws IOException;

  void delete(GcsItemId id, boolean recursive) throws IOException;

  void rename(GcsItemId src, GcsItemId dst) throws IOException;

  java.util.List<GcsItemInfo> listStatus(GcsItemId id) throws IOException;
}
