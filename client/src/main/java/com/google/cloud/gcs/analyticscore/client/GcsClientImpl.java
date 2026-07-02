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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.FolderName;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlSettings;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsClientImpl implements GcsClient {
  private static final Logger LOG = LoggerFactory.getLogger(GcsClientImpl.class);
  private static final List<Storage.BlobField> BLOB_METADATA_FIELDS =
      ImmutableList.of(Storage.BlobField.GENERATION, Storage.BlobField.SIZE);
  private static final String USER_AGENT_PREFIX = "gcs-analytics-core/";

  @VisibleForTesting Storage storage;
  private final GcsClientOptions clientOptions;
  private final Optional<Credentials> credentials;
  private Supplier<ExecutorService> executorServiceSupplier;
  private final Telemetry telemetry;
  private StorageControlClient storageControlClient;

  GcsClientImpl(
      Credentials credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry) {
    this(Optional.of(credentials), clientOptions, executorServiceSupplier, telemetry);
  }

  GcsClientImpl(
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry) {
    this(Optional.empty(), clientOptions, executorServiceSupplier, telemetry);
  }

  private GcsClientImpl(
      Optional<Credentials> credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry) {
    this.clientOptions = clientOptions;
    this.credentials = credentials;
    this.executorServiceSupplier = executorServiceSupplier;
    this.telemetry = telemetry;
    this.storage = createStorage(credentials);
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemInfo gcsItemInfo, GcsReadOptions readOptions) throws IOException {
    checkNotNull(gcsItemInfo, "itemInfo should not be null");
    checkNotNull(readOptions, "readOptions should not be null");
    checkArgument(
        gcsItemInfo.getItemId().isGcsObject(),
        "Expected GCS object to be provided. But got: " + gcsItemInfo.getItemId());

    return new GcsReadChannel(
        storage, gcsItemInfo, readOptions, executorServiceSupplier, telemetry);
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemId gcsItemId, GcsReadOptions readOptions) throws IOException {
    checkNotNull(gcsItemId, "gcsItemId should not be null");
    checkNotNull(readOptions, "readOptions should not be null");
    return new GcsReadChannel(storage, gcsItemId, readOptions, executorServiceSupplier, telemetry) {
      @Override
      public long size() throws IOException {
        if (itemInfo == null) {
          itemInfo = getGcsItemInfo(itemId);
          itemId = itemInfo.getItemId();
        }
        return itemInfo.getSize();
      }
    };
  }

  @Override
  public GcsItemInfo getGcsItemInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "Item ID must not be null.");
    if (itemId.isGcsObject()) {
      return getGcsObjectInfo(itemId);
    }
    throw new UnsupportedOperationException(
        String.format("Expected gcs object but got %s", itemId));
  }

  @Override
  public GcsItemInfo getBucketInfo(GcsItemId itemId) throws IOException {
    checkArgument(itemId.isBucket(), "Expected a bucket itemId");
    BucketInfo bucketInfo = storage.get(itemId.getBucketName());
    if (bucketInfo == null) {
      throw new IOException("Bucket not found: " + itemId.getBucketName());
    }
    return GcsItemInfo.builder().setItemId(itemId).setSize(0).setInferredDirectory(true).build();
  }

  @Override
  public GcsItemInfo getFolderInfo(GcsItemId itemId) throws IOException {
    checkArgument(itemId.isGcsObject(), "Expected a folder itemId");
    String objectName = itemId.getObjectName().orElse("");
    String folderName =
        objectName.endsWith("/") ? objectName.substring(0, objectName.length() - 1) : objectName;

    GetFolderRequest request =
        GetFolderRequest.newBuilder()
            .setName(FolderName.format("_", itemId.getBucketName(), folderName))
            .build();
    try {
      Folder folder = lazyGetStorageControlClient().getFolder(request);
      return GcsItemInfo.builder().setItemId(itemId).setSize(0).setNativeHnsFolder(true).build();
    } catch (Exception e) {
      throw new IOException("Folder not found: " + itemId, e);
    }
  }

  private StorageControlClient lazyGetStorageControlClient() throws IOException {
    if (this.storageControlClient == null) {
      StorageControlSettings.Builder builder = StorageControlSettings.newBuilder();
      this.credentials.ifPresent(
          c -> builder.setCredentialsProvider(FixedCredentialsProvider.create(c)));
      this.storageControlClient = StorageControlClient.create(builder.build());
    }
    return this.storageControlClient;
  }

  @Override
  public java.util.List<GcsItemInfo> listObjectInfo(GcsItemId prefixId, int maxResults)
      throws IOException {
    String prefix = prefixId.getObjectName().orElse("");
    com.google.api.gax.paging.Page<Blob> page =
        storage.list(
            prefixId.getBucketName(),
            Storage.BlobListOption.prefix(prefix),
            Storage.BlobListOption.pageSize(maxResults));

    ImmutableList.Builder<GcsItemInfo> builder = ImmutableList.builder();
    for (Blob blob : page.iterateAll()) {
      GcsItemId id =
          GcsItemId.builder()
              .setContentGeneration(blob.getGeneration())
              .setBucketName(blob.getBucket())
              .setObjectName(blob.getName())
              .build();
      builder.add(
          GcsItemInfo.builder()
              .setItemId(id)
              .setSize(blob.getSize())
              .setContentGeneration(blob.getGeneration())
              .build());
      if (builder.build().size() >= maxResults && maxResults > 0) {
        break;
      }
    }
    return builder.build();
  }

  @Override
  public BucketProperties getBucketProperties(String bucketName) throws IOException {
    checkNotNull(bucketName, "bucketName cannot be null");
    try {
      BucketInfo bucketInfo =
          storage.get(
              bucketName,
              Storage.BucketGetOption.fields(Storage.BucketField.HIERARCHICAL_NAMESPACE));
      if (bucketInfo == null) {
        throw new IOException("Bucket not found: " + bucketName);
      }
      boolean hnsEnabled =
          Optional.ofNullable(bucketInfo.getHierarchicalNamespace())
              .map(BucketInfo.HierarchicalNamespace::getEnabled)
              .orElse(false);
      return BucketProperties.create(hnsEnabled);
    } catch (StorageException storageException) {
      throw new IOException("Unable to access bucket: " + bucketName, storageException);
    }
  }

  @Override
  public void close() {
    try {
      storage.close();
    } catch (Exception e) {
      LOG.debug("Exception while closing storage instance", e);
    }
    if (storageControlClient != null) {
      try {
        storageControlClient.close();
      } catch (Exception e) {
        LOG.debug("Exception while closing storageControlClient", e);
      }
    }
  }

  @VisibleForTesting
  protected Storage createStorage(Optional<Credentials> credentials) {
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    String userAgent = getUserAgent();
    builder.setHeaderProvider(FixedHeaderProvider.create(ImmutableMap.of("User-Agent", userAgent)));
    clientOptions.getProjectId().ifPresent(builder::setProjectId);
    clientOptions.getClientLibToken().ifPresent(builder::setClientLibToken);
    clientOptions.getServiceHost().ifPresent(builder::setHost);
    credentials.ifPresent(builder::setCredentials);

    return builder.build().getService();
  }

  private String getVersion() {
    return VersionHelper.VERSION;
  }

  @VisibleForTesting
  String getUserAgent() {
    return USER_AGENT_PREFIX
        + getVersion()
        + clientOptions.getUserAgent().map(agent -> " " + agent).orElse("");
  }

  private GcsItemInfo getGcsObjectInfo(GcsItemId itemId) throws IOException {
    checkArgument(itemId.isGcsObject(), String.format("Expected gcs object got %s", itemId));
    Blob blob = getBlob(itemId.getBucketName(), itemId.getObjectName().get());
    if (blob == null) {
      throw new IOException("Object not found:" + itemId);
    }
    GcsItemId itemIdWithGeneration =
        GcsItemId.builder()
            .setContentGeneration(blob.getGeneration())
            .setBucketName(blob.getBucket())
            .setObjectName(blob.getName())
            .build();
    return GcsItemInfo.builder()
        .setItemId(itemIdWithGeneration)
        .setSize(blob.getSize())
        .setContentGeneration(blob.getGeneration())
        .build();
  }

  private Blob getBlob(String bucketName, String objectName) throws IOException {
    checkNotNull(bucketName);
    checkNotNull(objectName);
    BlobId blobId = BlobId.of(bucketName, objectName);
    try {
      return storage.get(
          blobId,
          Storage.BlobGetOption.fields(BLOB_METADATA_FIELDS.toArray(new Storage.BlobField[0])));
    } catch (StorageException storageException) {
      throw new IOException("Unable to access blob :" + blobId, storageException);
    }
  }
}
