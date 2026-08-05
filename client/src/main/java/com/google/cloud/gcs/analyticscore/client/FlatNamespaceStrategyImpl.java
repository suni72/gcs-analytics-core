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

package com.google.cloud.gcs.analyticscore.client;

final class FlatNamespaceStrategyImpl implements NamespaceStrategy {

  private final GcsClient gcsClient;
  private final java.util.concurrent.ExecutorService listExecutorService;

  FlatNamespaceStrategyImpl(
      GcsClient gcsClient, java.util.concurrent.ExecutorService listExecutorService) {
    this.gcsClient = gcsClient;
    this.listExecutorService = listExecutorService;
  }

  @Override
  public GcsItemInfo getFileInfo(GcsItemId id, PathType pathType) throws java.io.IOException {
    if (listExecutorService == null) {
      return getFileInfoSequential(id);
    }

    // Launch Parallel Prefix Scan (Max 1, trailing slash) + Direct Lookup for exact path
    String prefix = id.getObjectName().orElse("") + "/";
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(id.getBucketName()).setObjectName(prefix).build();

    java.util.concurrent.Future<GcsItemInfo> directFuture =
        listExecutorService.submit(() -> gcsClient.getGcsItemInfo(id));
    java.util.concurrent.Future<GcsItemInfo> prefixFuture =
        listExecutorService.submit(
            () -> {
              java.util.List<GcsItemInfo> list = gcsClient.listObjectInfo(prefixId, 1);
              if (!list.isEmpty()) {
                return GcsItemInfo.createInferredDirectory(id);
              }
              throw new java.io.IOException("Not found");
            });

    try {
      return directFuture.get();
    } catch (Exception e) {
      try {
        return prefixFuture.get();
      } catch (Exception ex) {
        if (e.getCause() instanceof java.io.IOException) {
          throw (java.io.IOException) e.getCause();
        }
        throw new java.io.IOException(e.getCause());
      }
    }
  }

  private GcsItemInfo getFileInfoSequential(GcsItemId id) throws java.io.IOException {
    try {
      return gcsClient.getGcsItemInfo(id);
    } catch (java.io.IOException e) {
      String prefix = id.getObjectName().orElse("") + "/";
      GcsItemId prefixId =
          GcsItemId.builder().setBucketName(id.getBucketName()).setObjectName(prefix).build();
      java.util.List<GcsItemInfo> list = gcsClient.listObjectInfo(prefixId, 1);
      if (!list.isEmpty()) {
        return GcsItemInfo.createInferredDirectory(id);
      }
      throw e;
    }
  }
}
