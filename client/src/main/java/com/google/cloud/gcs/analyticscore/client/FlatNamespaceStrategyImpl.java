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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class FlatNamespaceStrategyImpl implements NamespaceStrategy {

  private static final Logger logger = LoggerFactory.getLogger(FlatNamespaceStrategyImpl.class);

  private final GcsClient gcsClient;
  private final Supplier<ExecutorService> listExecutorServiceSupplier;

  FlatNamespaceStrategyImpl(
      GcsClient gcsClient, Supplier<ExecutorService> listExecutorServiceSupplier) {
    this.gcsClient = gcsClient;
    this.listExecutorServiceSupplier = listExecutorServiceSupplier;
  }

  @Override
  public GcsItemInfo getFileInfo(GcsItemId id, PathType pathType) throws IOException {
    if (pathType == PathType.ROOT || pathType == PathType.BUCKET) {
      throw new IllegalArgumentException("Path cannot be ROOT or BUCKET type");
    }

    ExecutorService listExecutorService = listExecutorServiceSupplier.get();

    // Start a background task to check if this path is an implicit directory (i.e. it has
    // children).
    Future<Boolean> implicitDirectoryFuture =
        listExecutorService.submit(() -> isImplicitDirectory(id));

    try {
      // return direct object metadata if found
      return gcsClient.getGcsItemInfo(id);
    } catch (IOException directException) {
      // The direct object was not found. Wait for the background task to see if it's an implicit
      // directory instead.
      try {
        if (implicitDirectoryFuture.get()) {
          return GcsItemInfo.createInferredDirectory(id);
        }
      } catch (Exception fallbackException) {
        // If the background thread is interrupted or fails, attach the error to the main exception
        // for visibility
        directException.addSuppressed(fallbackException);
      }

      throw directException;
    }
  }

  // Checks if a path is an implicit directory by seeing if any objects exist with it as a prefix
  private boolean isImplicitDirectory(GcsItemId id) {
    String prefix = id.getObjectName().orElse("") + "/";
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(id.getBucketName()).setObjectName(prefix).build();
    try {
      return !gcsClient.listObjectInfo(prefixId, 1).isEmpty();
    } catch (IOException e) {
      logger.warn("Failed to check if {} is an implicit directory, returning false", id, e);
      return false;
    }
  }
}
