/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client.namespace;

import com.google.cloud.gcs.analyticscore.client.GcsClient;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.common.PathType;
import java.io.IOException;

public class HierarchicalNamespaceStrategyImpl implements NamespaceStrategy {
  private final GcsClient gcsClient;
  private final FlatNamespaceStrategyImpl flatStrategyFallback;

  public HierarchicalNamespaceStrategyImpl(GcsClient gcsClient) {
    this.gcsClient = gcsClient;
    this.flatStrategyFallback = new FlatNamespaceStrategyImpl(gcsClient);
  }

  @Override
  public GcsItemInfo getFileInfo(GcsItemId id, PathType pathType) throws IOException {
    if (!id.isGcsObject()) {
      return flatStrategyFallback.getFileInfo(id, pathType);
    }

    GcsItemId dirId = id.toDirectoryId();
    try {
      GcsItemInfo folderInfo = gcsClient.getFolderMetadata(dirId);
      if (folderInfo != null && folderInfo.getSize() != -1L) {
        return folderInfo;
      }
    } catch (IOException e) {
      // Ignore and fallback to flat strategy
    }

    return flatStrategyFallback.getFileInfo(id, pathType);
  }

  @Override
  public void mkdirs(GcsItemId id) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void delete(GcsItemId id, boolean recursive) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void rename(GcsItemId src, GcsItemId dst) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public java.util.List<GcsItemInfo> listStatus(GcsItemId id) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
