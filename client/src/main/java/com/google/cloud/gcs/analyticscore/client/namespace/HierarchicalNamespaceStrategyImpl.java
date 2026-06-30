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
import com.google.cloud.gcs.analyticscore.client.PathType;
import java.io.IOException;

public class HierarchicalNamespaceStrategyImpl implements NamespaceStrategy {

  private final GcsClient gcsClient;

  public HierarchicalNamespaceStrategyImpl(GcsClient gcsClient) {
    this.gcsClient = gcsClient;
  }

  @Override
  public GcsItemInfo getFileInfo(GcsItemId id, PathType pathType) throws IOException {
    String objectName = id.getObjectName().orElse("");
    String dirPrefix = objectName.endsWith("/") ? objectName : objectName + "/";

    GcsItemId folderId =
        GcsItemId.builder().setBucketName(id.getBucketName()).setObjectName(dirPrefix).build();

    try {
      return gcsClient.getFolderInfo(folderId);
    } catch (IOException e) {
      // Fallback
      GcsItemId objectId =
          GcsItemId.builder()
              .setBucketName(id.getBucketName())
              .setObjectName(
                  objectName.endsWith("/")
                      ? objectName.substring(0, objectName.length() - 1)
                      : objectName)
              .build();
      try {
        return gcsClient.getGcsItemInfo(objectId);
      } catch (IOException ex) {
        throw new IOException("File not found: " + id, ex);
      }
    }
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
