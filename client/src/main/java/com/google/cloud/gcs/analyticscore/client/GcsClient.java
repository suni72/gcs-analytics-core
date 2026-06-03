/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.client;

import com.google.cloud.gcs.analyticscore.common.BucketCapabilities;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@VisibleForTesting
public interface GcsClient {
  /** Opens a new read channel. */
  VectoredSeekableByteChannel openReadChannel(GcsItemInfo itemInfo, GcsReadOptions readOptions)
      throws IOException;

  /** Opens a new read channel. */
  VectoredSeekableByteChannel openReadChannel(GcsItemId itemId, GcsReadOptions readOptions)
      throws IOException;

  /** Fetches object metadata. */
  GcsItemInfo getGcsItemInfo(GcsItemId itemId) throws IOException;

  void copyObject(GcsItemId src, GcsItemId dst) throws IOException;

  void deleteObjects(List<GcsItemId> ids) throws IOException;

  void updateObjectMetadata(GcsItemId id, Map<String, byte[]> metadata) throws IOException;

  GcsItemInfo getFolderMetadata(GcsItemId id) throws IOException;

  void createFolder(GcsItemId id) throws IOException;

  void deleteFolder(GcsItemId id) throws IOException;

  void renameFolder(GcsItemId src, GcsItemId dst) throws IOException;

  BucketCapabilities getBucketCapabilities(String bucketName) throws IOException;

  /** Close the client. */
  void close();
}
