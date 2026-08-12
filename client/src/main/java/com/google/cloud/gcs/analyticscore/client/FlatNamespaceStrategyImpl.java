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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

final class FlatNamespaceStrategyImpl implements NamespaceStrategy {
  private final GcsClient gcsClient;

  FlatNamespaceStrategyImpl(GcsClient gcsClient) {
    this.gcsClient = gcsClient;
  }

  @Override
  public void createDirectory(GcsItemId id) throws IOException {
    checkNotNull(id, "id must not be null");
    checkArgument(id.isGcsObject(), "Expected a directory object itemId but got: %s", id);
    String objectName = UriUtil.ensureTrailingSlash(id.getObjectName().orElse(""));
    GcsItemId dirItemId =
        GcsItemId.builder().setBucketName(id.getBucketName()).setObjectName(objectName).build();
    try {
      gcsClient.createEmptyObject(dirItemId);
    } catch (FileAlreadyExistsException e) {
      // Directory marker already exists, ignore.
    }
  }
}
