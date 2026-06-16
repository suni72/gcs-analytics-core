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
import java.util.List;

public class FlatNamespaceStrategyImpl implements NamespaceStrategy {
  private final GcsClient gcsClient;

  public FlatNamespaceStrategyImpl(GcsClient gcsClient) {
    this.gcsClient = gcsClient;
  }

  @Override
  public GcsItemInfo getFileInfo(GcsItemId id, PathType pathType) throws IOException {
    if (!id.isGcsObject()) {
      return gcsClient.getGcsItemInfo(id);
    }

    if (pathType != PathType.DIRECTORY && !id.isDirectory()) {
      GcsItemInfo objectInfo = gcsClient.getGcsItemInfo(id);
      if (objectInfo != null && objectInfo.getSize() != -1L) {
        return objectInfo;
      }
    }

    GcsItemId dirId = id.toDirectoryId();
    List<GcsItemInfo> listedObjects = gcsClient.listObjects(dirId, 1);
    if (!listedObjects.isEmpty()) {
      return GcsItemInfo.builder().setItemId(dirId).setSize(0L).setInferredDirectory(true).build();
    }

    return GcsItemInfo.builder().setSize(-1L).build();
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
