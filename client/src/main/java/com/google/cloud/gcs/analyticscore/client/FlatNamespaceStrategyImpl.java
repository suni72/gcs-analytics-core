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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class FlatNamespaceStrategyImpl implements NamespaceStrategy {

  private final GcsClient gcsClient;
  private final Supplier<ExecutorService> statusExecutorServiceSupplier;

  public FlatNamespaceStrategyImpl(
      GcsClient gcsClient, Supplier<ExecutorService> statusExecutorServiceSupplier) {
    this.gcsClient = gcsClient;
    this.statusExecutorServiceSupplier = statusExecutorServiceSupplier;
  }

  @Override
  public GcsItemInfo getFileInfo(GcsItemId id, PathType pathType) throws IOException {
    String objectName = id.getObjectName().orElse("");
    String dirPrefix = UriUtil.ensureTrailingSlash(objectName);

    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(id.getBucketName()).setObjectName(dirPrefix).build();

    if (pathType == PathType.DIRECTORY) {
      List<GcsItemInfo> children = gcsClient.listObjectInfo(prefixId, 1);
      if (children != null && !children.isEmpty()) {
        return GcsItemInfo.builder()
            .setItemId(prefixId)
            .setSize(0)
            .setInferredDirectory(true)
            .build();
      }
      throw new java.io.FileNotFoundException("File not found: " + id);
    }

    Future<List<GcsItemInfo>> prefixScanFuture =
        statusExecutorServiceSupplier.get().submit(() -> gcsClient.listObjectInfo(prefixId, 1));

    try {
      GcsItemInfo directInfo = gcsClient.getGcsItemInfo(id);
      prefixScanFuture.cancel(true);
      return directInfo;
    } catch (IOException e) {
      try {
        List<GcsItemInfo> children = prefixScanFuture.get();
        if (children != null && !children.isEmpty()) {
          return GcsItemInfo.builder()
              .setItemId(prefixId)
              .setSize(0)
              .setInferredDirectory(true)
              .build();
        }
      } catch (Exception ex) {
        // Interrupted or ExecutionException
      }
      throw new java.io.FileNotFoundException("File not found: " + id);
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
