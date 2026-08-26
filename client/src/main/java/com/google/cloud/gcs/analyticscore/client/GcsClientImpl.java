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
import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.client.GcsReadChannel.ItemInfoProvider;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.HierarchicalNamespace;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.FutureCallback;
import com.google.protobuf.Timestamp;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.FolderName;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlSettings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsClientImpl implements GcsClient {
  private static final Logger LOG = LoggerFactory.getLogger(GcsClientImpl.class);
  private static final List<BlobField> BLOB_METADATA_FIELDS =
      ImmutableList.of(
          BlobField.NAME,
          BlobField.BUCKET,
          BlobField.GENERATION,
          BlobField.METAGENERATION,
          BlobField.SIZE,
          BlobField.CONTENT_TYPE,
          BlobField.CONTENT_ENCODING,
          BlobField.TIME_CREATED,
          BlobField.UPDATED,
          BlobField.MD5HASH,
          BlobField.CRC32C,
          BlobField.METADATA);
  private static final List<BucketField> BUCKET_METADATA_FIELDS =
      ImmutableList.of(
          BucketField.NAME,
          BucketField.LOCATION,
          BucketField.METAGENERATION,
          BucketField.TIME_CREATED,
          BucketField.UPDATED);
  private static final String USER_AGENT_PREFIX = "gcs-analytics-core/";
  private static final String RAPID_STORAGE_CLASS = "RAPID";
  private static final GcsWriteOptions EMPTY_OBJECT_WRITE_OPTIONS =
      GcsWriteOptions.builder().setEnsureEmptyObjectsMetadataMatch(false).build();
  private static final ImmutableSet<Integer> RETRYABLE_GET_METADATA_ERROR_CODES =
      ImmutableSet.of(429, 500, 502, 503, 504);

  @VisibleForTesting Storage storage;
  @VisibleForTesting volatile Storage grpcStorage;
  private final GcsClientOptions clientOptions;
  private final Optional<Credentials> credentials;
  private Supplier<ExecutorService> executorServiceSupplier;
  private final Telemetry telemetry;
  @VisibleForTesting volatile StorageControlClient storageControlClient;

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

    if (readOptions.isBidiReadEnabled()) {
      return new GcsBidiReadChannel(
          lazyGetGrpcStorage(), gcsItemInfo, readOptions, executorServiceSupplier, telemetry);
    }

    return new GcsReadChannel(
        storage, gcsItemInfo, readOptions, executorServiceSupplier, telemetry);
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemId gcsItemId, GcsReadOptions readOptions) throws IOException {
    checkNotNull(gcsItemId, "gcsItemId should not be null");
    checkNotNull(readOptions, "readOptions should not be null");
    ItemInfoProvider itemInfoProvider = this::getGcsItemInfo;
    if (readOptions.isBidiReadEnabled()) {
      return new GcsBidiReadChannel(
          lazyGetGrpcStorage(),
          gcsItemId,
          readOptions,
          executorServiceSupplier,
          telemetry,
          itemInfoProvider);
    } else {
      return new GcsReadChannel(
          storage, gcsItemId, readOptions, executorServiceSupplier, telemetry, itemInfoProvider);
    }
  }

  @Override
  public WritableByteChannel createWriteChannel(GcsItemId itemId, GcsWriteOptions writeOptions)
      throws IOException {
    checkNotNull(itemId, "itemId should not be null");

    BlobInfo blobInfo = createBlobInfo(itemId, writeOptions);

    try {
      BlobWriteOption[] sdkWriteOptions =
          Optional.ofNullable(writeOptions)
              .orElseGet(() -> GcsWriteOptions.builder().build())
              .generateWriteOptions(itemId);
      BlobWriteSession sdkWriteSession = storage.blobWriteSession(blobInfo, sdkWriteOptions);
      WritableByteChannel channel = sdkWriteSession.open();
      return new GcsWriteChannel(sdkWriteSession, channel, blobInfo, writeOptions);
    } catch (StorageException | IOException e) {
      throw GcsExceptionUtil.translateWriteException(
          e, "initialization", blobInfo.getBlobId(), 0L, writeOptions);
    }
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
  public List<GcsItemInfo> getGcsObjectInfos(List<GcsItemId> itemIds) throws IOException {
    checkNotNull(itemIds, "itemIds must not be null");
    if (itemIds.isEmpty()) {
      return Collections.emptyList();
    }
    if (itemIds.size() == 1) {
      GcsItemInfo info = getGcsObjectInfoOrNull(itemIds.get(0));
      return Collections.singletonList(info);
    }

    GcsItemInfo[] results = new GcsItemInfo[itemIds.size()];
    Set<IOException> innerExceptions = ConcurrentHashMap.newKeySet();
    int numThreads = Math.min(itemIds.size(), clientOptions.getBatchThreads());
    BatchExecutor executor = new BatchExecutor(numThreads);

    try {
      for (int i = 0; i < itemIds.size(); i++) {
        final int index = i;
        final GcsItemId itemId = itemIds.get(i);
        executor.queue(
            () -> getGcsObjectInfoOrNull(itemId),
            new FutureCallback<GcsItemInfo>() {
              @Override
              public void onSuccess(GcsItemInfo result) {
                results[index] = result;
              }

              @Override
              public void onFailure(Throwable t) {
                innerExceptions.add(
                    new IOException(String.format("Error getting %s object", itemId), t));
              }
            });
      }
    } finally {
      executor.shutdown();
    }

    if (!innerExceptions.isEmpty()) {
      throw innerExceptions.iterator().next();
    }

    return Arrays.asList(results);
  }

  private GcsItemInfo getGcsObjectInfoOrNull(GcsItemId itemId) throws IOException {
    try {
      return getGcsObjectInfo(itemId);
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  @Override
  public GcsItemInfo getBucketInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "Item ID must not be null.");
    checkArgument(itemId.isBucket(), "Expected a bucket itemId");
    BucketInfo bucketInfo;
    try {
      bucketInfo =
          storage.get(
              itemId.getBucketName(),
              Storage.BucketGetOption.fields(BUCKET_METADATA_FIELDS.toArray(new BucketField[0])));
    } catch (StorageException e) {
      if (e.getCode() == 404) {
        bucketInfo = null;
      } else {
        throw new IOException("Unable to access bucket: " + itemId.getBucketName(), e);
      }
    }
    if (bucketInfo == null) {
      return GcsItemInfo.createNotFound(itemId);
    }
    return fromBucketInfo(bucketInfo);
  }

  @Override
  public GcsItemInfo getFolderInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "Item ID must not be null.");
    if (itemId.isRoot()) {
      return GcsItemInfo.ROOT_INFO;
    }
    if (itemId.isBucket()) {
      return getBucketInfo(itemId);
    }
    checkArgument(itemId.isGcsObject(), "Expected a folder itemId");
    String objectName = itemId.getObjectName().orElse("");
    String folderName = UriUtil.removeTrailingSlash(objectName);
    checkArgument(!folderName.isEmpty(), "Folder name cannot be empty");

    GetFolderRequest request =
        GetFolderRequest.newBuilder()
            .setName(FolderName.format("_", itemId.getBucketName(), folderName))
            .build();
    try {
      Folder folder = lazyGetStorageControlClient().getFolder(request);
      return fromFolder(folder, itemId);
    } catch (NotFoundException e) {
      return GcsItemInfo.createNotFound(itemId);
    } catch (Exception e) {
      throw new IOException("Failed to get folder info for: " + itemId, e);
    }
  }

  @VisibleForTesting
  StorageControlClient lazyGetStorageControlClient() throws IOException {
    StorageControlClient result = this.storageControlClient;
    if (result == null) {
      synchronized (this) {
        result = this.storageControlClient;
        if (result == null) {
          this.storageControlClient = result = createStorageControlClient(this.credentials);
        }
      }
    }
    return result;
  }

  @VisibleForTesting
  protected StorageControlClient createStorageControlClient(Optional<Credentials> credentials)
      throws IOException {
    StorageControlSettings.Builder builder = StorageControlSettings.newBuilder();
    credentials.ifPresent(c -> builder.setCredentialsProvider(FixedCredentialsProvider.create(c)));
    return StorageControlClient.create(builder.build());
  }

  @Override
  public List<GcsItemInfo> listFirstObjectWithPrefix(GcsItemId prefixId) throws IOException {
    checkNotNull(prefixId, "prefixId must not be null");
    String prefix = prefixId.getObjectName().orElse("");

    try {
      Page<Blob> page =
          storage.list(
              prefixId.getBucketName(),
              BlobListOption.prefix(prefix),
              BlobListOption.pageSize(1),
              BlobListOption.fields(BLOB_METADATA_FIELDS.toArray(new BlobField[0])));

      for (Blob blob : page.getValues()) {
        return ImmutableList.of(fromBlob(blob));
      }
      return ImmutableList.of();
    } catch (StorageException e) {
      if (e.getCode() == 404) {
        throw new FileNotFoundException("Bucket not found: " + prefixId.getBucketName());
      }
      throw new IOException("Failed to list the first object for prefix: " + prefixId, e);
    }
  }

  private static GcsItemInfo fromBlob(Blob blob) {
    GcsItemId.Builder idBuilder =
        GcsItemId.builder().setBucketName(blob.getBucket()).setObjectName(blob.getName());
    Optional.ofNullable(blob.getGeneration()).ifPresent(idBuilder::setContentGeneration);
    GcsItemId id = idBuilder.build();

    GcsItemInfo.Builder infoBuilder =
        GcsItemInfo.builder()
            .setItemId(id)
            .setSize(blob.getSize() == null ? 0L : blob.getSize())
            .setCreationTime(toEpochMilli(blob.getCreateTimeOffsetDateTime()))
            .setModificationTime(toEpochMilli(blob.getUpdateTimeOffsetDateTime()))
            .setVerificationAttributes(
                VerificationAttributes.create(
                    blob.getMd5() != null ? BaseEncoding.base64().decode(blob.getMd5()) : null,
                    blob.getCrc32c() != null
                        ? BaseEncoding.base64().decode(blob.getCrc32c())
                        : null));

    Optional.ofNullable(blob.getGeneration()).ifPresent(infoBuilder::setContentGeneration);
    Optional.ofNullable(blob.getContentType()).ifPresent(infoBuilder::setContentType);
    Optional.ofNullable(blob.getContentEncoding()).ifPresent(infoBuilder::setContentEncoding);
    Optional.ofNullable(blob.getMetageneration()).ifPresent(infoBuilder::setMetaGeneration);

    if (blob.isDirectory()) {
      infoBuilder.setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
    }

    infoBuilder.setExtendedAttributes(GcsItemInfo.decodeMetadata(blob.getMetadata()));

    return infoBuilder.build();
  }

  private static GcsItemInfo fromBucketInfo(BucketInfo bucketInfo) {
    GcsItemId itemId = GcsItemId.builder().setBucketName(bucketInfo.getName()).build();
    GcsItemInfo.Builder builder =
        GcsItemInfo.createBucket(itemId).toBuilder()
            .setCreationTime(toEpochMilli(bucketInfo.getCreateTimeOffsetDateTime()))
            .setModificationTime(toEpochMilli(bucketInfo.getUpdateTimeOffsetDateTime()));

    Optional.ofNullable(bucketInfo.getLocation()).ifPresent(builder::setLocation);
    Optional.ofNullable(bucketInfo.getMetageneration()).ifPresent(builder::setMetaGeneration);
    return builder.build();
  }

  private static GcsItemInfo fromFolder(Folder folder, GcsItemId itemId) {
    return GcsItemInfo.createFolder(
        itemId,
        toEpochMilli(folder.hasCreateTime() ? folder.getCreateTime() : null),
        toEpochMilli(folder.hasUpdateTime() ? folder.getUpdateTime() : null),
        folder.getMetageneration());
  }

  private static long toEpochMilli(OffsetDateTime dateTime) {
    return dateTime != null ? dateTime.toInstant().toEpochMilli() : 0L;
  }

  private static long toEpochMilli(Timestamp timestamp) {
    return timestamp != null
        ? Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toEpochMilli()
        : 0L;
  }

  BucketProperties getBucketProperties(String bucketName) throws IOException {
    checkNotNull(bucketName, "bucketName cannot be null");
    try {
      BucketInfo bucketInfo =
          storage.get(
              bucketName,
              Storage.BucketGetOption.fields(
                  Storage.BucketField.HIERARCHICAL_NAMESPACE, Storage.BucketField.STORAGE_CLASS));
      if (bucketInfo == null) {
        LOG.warn("Bucket {} not found, HNS and RAPID features will be disabled", bucketName);
        return BucketProperties.create(false, false);
      }
      boolean hnsEnabled =
          Optional.ofNullable(bucketInfo.getHierarchicalNamespace())
              .map(HierarchicalNamespace::getEnabled)
              .orElse(false);
      boolean isRapid =
          bucketInfo.getStorageClass() != null
              && RAPID_STORAGE_CLASS.equalsIgnoreCase(bucketInfo.getStorageClass().toString());
      return BucketProperties.create(hnsEnabled, isRapid);
    } catch (StorageException storageException) {
      if (storageException.getCode() == 403) {
        LOG.warn(
            "Access to bucket {} is forbidden (403), HNS and RAPID features will be disabled",
            bucketName);
        return BucketProperties.create(false, false);
      }
      throw new IOException("Unable to access bucket: " + bucketName, storageException);
    }
  }

  @Override
  public boolean isHnsBucket(String bucketName) throws IOException {
    return getBucketProperties(bucketName).isHnsEnabled();
  }

  @Override
  public void close() {
    try {
      storage.close();
    } catch (Exception e) {
      LOG.debug("Exception while closing storage instance", e);
    }
    synchronized (this) {
      if (grpcStorage != null) {
        try {
          grpcStorage.close();
        } catch (Exception e) {
          LOG.debug("Exception while closing grpcStorage instance", e);
        }
        grpcStorage = null;
      }
      if (storageControlClient != null) {
        try {
          storageControlClient.close();
        } catch (Exception e) {
          LOG.debug("Exception while closing storageControlClient", e);
        }
        storageControlClient = null;
      }
    }
  }

  @Override
  public void createBucket(String bucketName) throws IOException {
    checkNotNull(bucketName, "bucketName must not be null");
    checkArgument(!bucketName.isEmpty(), "bucketName must not be empty");
    try {
      // TODO: Support creation of HNS bucket when the HNS flag is on.
      storage.create(BucketInfo.of(bucketName));
    } catch (StorageException e) {
      if (e.getCode() == 409) {
        throw (FileAlreadyExistsException)
            new FileAlreadyExistsException("Bucket already exists: " + bucketName).initCause(e);
      }
      throw new IOException("Failed to create bucket: " + bucketName, e);
    }
  }

  @Override
  public void createEmptyObject(GcsItemId itemId) throws IOException {
    createEmptyObject(itemId, EMPTY_OBJECT_WRITE_OPTIONS);
  }

  @Override
  public void createEmptyObject(GcsItemId itemId, GcsWriteOptions options) throws IOException {
    checkNotNull(itemId, "itemId must not be null");
    checkArgument(itemId.isGcsObject(), "Expected a GCS object itemId but got: " + itemId);
    try {
      createEmptyObjectInternal(itemId, options);
    } catch (StorageException e) {
      if (canIgnoreExceptionForEmptyObject(e, itemId, options)) {
        LOG.info("Ignored exception while creating empty object", e);
      } else {
        if (e.getCode() == 409 || e.getCode() == 412) {
          throw (FileAlreadyExistsException)
              new FileAlreadyExistsException("Object already exists: " + itemId).initCause(e);
        }
        throw new IOException("Failed to create empty object: " + itemId, e);
      }
    }
  }

  private BlobInfo buildEmptyBlobInfo(GcsItemId itemId, GcsWriteOptions options) {
    BlobInfo.Builder blobInfoBuilder =
        BlobInfo.newBuilder(BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()));
    if (!options.getMetadata().isEmpty()) {
      blobInfoBuilder.setMetadata(GcsItemInfo.encodeMetadata(options.getMetadata()));
    }
    options.getContentType().ifPresent(blobInfoBuilder::setContentType);
    options.getContentEncoding().ifPresent(blobInfoBuilder::setContentEncoding);
    return blobInfoBuilder.build();
  }

  private void createEmptyObjectInternal(GcsItemId itemId, GcsWriteOptions options)
      throws IOException {
    BlobInfo blobInfo = buildEmptyBlobInfo(itemId, options);

    List<BlobTargetOption> targetOptions = new ArrayList<>();
    if (options.isDisableGzipContent()) {
      targetOptions.add(BlobTargetOption.disableGzipContent());
    }
    if (itemId.getContentGeneration().isPresent()) {
      targetOptions.add(BlobTargetOption.generationMatch(itemId.getContentGeneration().get()));
    } else if (itemId.isDirectory() || !options.isOverwriteExisting()) {
      targetOptions.add(BlobTargetOption.doesNotExist());
    }
    if (options.getEncryptionKey().isPresent()) {
      targetOptions.add(BlobTargetOption.encryptionKey(options.getEncryptionKey().get()));
    }

    if (isRapidBucket(itemId.getBucketName())) {
      createAppendableEmptyObject(itemId, options);
    } else {
      storage.create(blobInfo, targetOptions.toArray(new BlobTargetOption[0]));
    }
  }

  private boolean isRapidBucket(String bucketName) throws IOException {
    return getBucketProperties(bucketName).isRapid();
  }

  private void createAppendableEmptyObject(GcsItemId itemId, GcsWriteOptions options)
      throws IOException {
    checkNotNull(itemId, "itemId must not be null");
    checkArgument(itemId.isGcsObject(), "Expected a GCS object itemId but got: " + itemId);
    BlobInfo blobInfo = buildEmptyBlobInfo(itemId, options);
    BlobWriteOption[] writeOptions =
        options.toBuilder().setOverwriteExisting(false).build().generateWriteOptions(itemId);
    try {
      BlobAppendableUpload upload =
          lazyGetGrpcStorage()
              .blobAppendableUpload(blobInfo, BlobAppendableUploadConfig.of(), writeOptions);
      try (AppendableUploadWriteableByteChannel channel = upload.open()) {
        channel.write(ByteBuffer.wrap(new byte[0]));
        channel.finalizeAndClose();
      }
    } catch (IOException e) {
      if (e.getCause() instanceof StorageException) {
        throw (StorageException) e.getCause();
      }
      throw e;
    }
  }

  /**
   * Determines whether an exception thrown during 0-byte empty object creation can be safely
   * ignored. If {@link GcsWriteOptions#isEnsureEmptyObjectsMetadataMatch()} is enabled, it also
   * verifies that all requested custom metadata values match the existing object's attributes.
   *
   * <p>When creating empty objects, concurrent requests (triggering 412 - Precondition Failed when
   * precondition {@code doesNotExist()} is set on a directory marker) or transient GCS errors (429
   * - Too Many Requests, 500 - Internal Server Error, or 503 - Service Unavailable) can cause
   * object creation to fail.
   */
  private boolean canIgnoreExceptionForEmptyObject(
      StorageException exceptionOnCreate, GcsItemId itemId, GcsWriteOptions options)
      throws IOException {
    int code = exceptionOnCreate.getCode();
    if (code == 429 || code == 500 || code == 503 || (itemId.isDirectory() && code == 412)) {
      long maxWaitTimeMillis = clientOptions.getMaxWaitTimeForEmptyObjectCreation().toMillis();
      return pollWithExponentialBackoff(
          () -> {
            Blob blob;
            try {
              blob = getBlob(itemId.getBucketName(), itemId.getObjectName().orElse(""));
            } catch (IOException e) {
              if (e.getCause() instanceof StorageException) {
                int errorCode = ((StorageException) e.getCause()).getCode();
                if (RETRYABLE_GET_METADATA_ERROR_CODES.contains(errorCode)) {
                  // returns false so that the retry logic in pollWithExponentialBackoff continues
                  return false;
                }
              }
              exceptionOnCreate.addSuppressed(e);
              throw new IOException(
                  "Failed to verify existence of 0-byte object "
                      + itemId
                      + " after creation failed: ",
                  exceptionOnCreate);
            }
            if (blob != null) {
              if (blob.getSize() == null || blob.getSize() == 0L) {
                if (options.isEnsureEmptyObjectsMetadataMatch()) {
                  GcsItemInfo existingInfo = fromBlob(blob);
                  return GcsItemInfo.isMetadataEqual(
                      options.getMetadata(), existingInfo.getExtendedAttributes());
                }
                return true;
              }
            }
            return false;
          },
          /*max elapsed time*/ maxWaitTimeMillis,
          /*initial interval*/ 100L,
          /*max interval*/ 500L,
          /*multiplier*/ 1.5,
          /*randomization factor*/ 0.15);
    }
    return false;
  }

  @FunctionalInterface
  private interface ThrowingBooleanSupplier {
    boolean getAsBoolean() throws IOException;
  }

  /**
   * Evaluates a condition using capped exponential backoff with randomization jitter until it
   * evaluates to true or timeout occurs.
   *
   * <p>Reference implementation adapted from hadoop-connectors: {@code
   * com.google.cloud.hadoop.gcsio.GoogleCloudStorageClientImpl#canIgnoreExceptionForEmptyObject}
   * and {@code com.google.api.client.util.ExponentialBackOff}.
   *
   * @param predicate The condition to evaluate on each iteration. May throw IOException.
   * @param maxElapsedTimeMillis Maximum wall-clock time to wait in milliseconds.
   * @param initialIntervalMillis Initial sleep interval in milliseconds.
   * @param maxIntervalMillis Maximum sleep interval in milliseconds.
   * @param multiplier Multiplier for exponential backoff.
   * @param randomizationFactor Randomization jitter factor (e.g., 0.15 for +/- 15% jitter).
   * @return true if predicate returned true within the timeout, false otherwise.
   * @throws IOException if the predicate throws an IOException (failing fast).
   */
  private static boolean pollWithExponentialBackoff(
      ThrowingBooleanSupplier predicate,
      long maxElapsedTimeMillis,
      long initialIntervalMillis,
      long maxIntervalMillis,
      double multiplier,
      double randomizationFactor)
      throws IOException {
    long startTimeNanos = System.nanoTime();
    long sleepInterval = initialIntervalMillis;

    while (true) {
      if (predicate.getAsBoolean()) {
        return true;
      }

      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
      long remainingMillis = maxElapsedTimeMillis - elapsedMillis;
      if (remainingMillis <= 0) {
        break;
      }

      // Inject randomization jitter to prevent thundering herd retry synchronization.
      // Reference: com.google.api.client.util.ExponentialBackOff
      double jitter =
          1.0 + (ThreadLocalRandom.current().nextDouble() * 2.0 - 1.0) * randomizationFactor;
      long randomizedSleep = Math.max(1L, (long) (sleepInterval * jitter));
      long nextSleep = Math.min(randomizedSleep, remainingMillis);

      try {
        Thread.sleep(nextSleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      sleepInterval = Math.min(maxIntervalMillis, (long) (sleepInterval * multiplier));
    }

    return false;
  }

  @Override
  public void createFolder(GcsItemId itemId, boolean recursive) throws IOException {
    checkNotNull(itemId, "itemId must not be null");
    checkArgument(itemId.isGcsObject(), "Expected a folder itemId but got: " + itemId);
    String objectName = itemId.getObjectName().orElse("");
    String folderName = UriUtil.removeTrailingSlash(objectName);
    checkArgument(!folderName.isEmpty(), "Folder name cannot be empty");
    CreateFolderRequest request =
        CreateFolderRequest.newBuilder()
            .setParent(String.format("projects/_/buckets/%s", itemId.getBucketName()))
            .setFolderId(folderName)
            .setRecursive(recursive)
            .build();
    try {
      lazyGetStorageControlClient().createFolder(request);
    } catch (AlreadyExistsException e) {
      throw (FileAlreadyExistsException)
          new FileAlreadyExistsException("Folder already exists: " + itemId).initCause(e);
    } catch (Exception e) {
      throw new IOException("Failed to create folder: " + itemId, e);
    }
  }

  @VisibleForTesting
  Storage lazyGetGrpcStorage() {
    if (clientOptions.getGcsReadOptions().isBidiReadEnabled()) {
      return this.storage;
    }
    Storage result = this.grpcStorage;
    if (result == null) {
      synchronized (this) {
        result = this.grpcStorage;
        if (result == null) {
          this.grpcStorage = result = createGrpcStorage(this.credentials);
        }
      }
    }
    return result;
  }

  @VisibleForTesting
  protected Storage createGrpcStorage(Optional<Credentials> credentials) {
    return buildStorage(StorageOptions.grpc(), credentials);
  }

  @VisibleForTesting
  protected Storage createStorage(Optional<Credentials> credentials) {
    return buildStorage(
        clientOptions.getGcsReadOptions().isBidiReadEnabled()
            ? StorageOptions.grpc()
            : StorageOptions.newBuilder(),
        credentials);
  }

  private Storage buildStorage(StorageOptions.Builder builder, Optional<Credentials> credentials) {
    String userAgent = getUserAgent();
    builder.setHeaderProvider(FixedHeaderProvider.create(ImmutableMap.of("User-Agent", userAgent)));
    clientOptions.getProjectId().ifPresent(builder::setProjectId);
    clientOptions.getClientLibToken().ifPresent(builder::setClientLibToken);
    clientOptions.getServiceHost().ifPresent(builder::setHost);
    credentials.ifPresent(builder::setCredentials);
    builder.setBlobWriteSessionConfig(clientOptions.generateSessionConfig());

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
      return GcsItemInfo.createNotFound(itemId);
    }
    return fromBlob(blob);
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

  private static BlobInfo createBlobInfo(GcsItemId itemId, GcsWriteOptions writeOptions) {
    checkNotNull(itemId, "itemId should not be null");
    String objectName =
        itemId
            .getObjectName()
            .orElseThrow(() -> new IllegalArgumentException("Object name must be present"));
    BlobId blobId =
        itemId
            .getContentGeneration()
            .map(generation -> BlobId.of(itemId.getBucketName(), objectName, generation))
            .orElseGet(() -> BlobId.of(itemId.getBucketName(), objectName));

    BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);

    Optional.ofNullable(writeOptions)
        .ifPresent(
            options -> {
              options.getContentType().ifPresent(blobInfoBuilder::setContentType);
              options.getContentEncoding().ifPresent(blobInfoBuilder::setContentEncoding);
              ImmutableMap<String, byte[]> metadata = options.getMetadata();
              if (!metadata.isEmpty()) {
                blobInfoBuilder.setMetadata(GcsItemInfo.encodeMetadata(metadata));
              }
            });

    return blobInfoBuilder.build();
  }
}
