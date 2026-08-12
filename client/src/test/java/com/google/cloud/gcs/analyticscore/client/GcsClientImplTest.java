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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.BlobWriteSessionConfig;
import com.google.cloud.storage.BlobWriteSessionConfigs;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.HierarchicalNamespace;
import com.google.cloud.storage.BufferToDiskThenUpload;
import com.google.cloud.storage.DefaultBlobWriteSessionConfig;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.ParallelCompositeUploadBlobWriteSessionConfig;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.GetFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlSettings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class GcsClientImplTest {

  private static final String TEST_PROJECT = "test-project";
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";
  private static final String TEST_BUCKET_NAME = "test-bucket-name";
  private static final String TEST_BUCKET_ID = "test-bucket-id";
  private static final String TEST_OBJECT_ID = "test-object-id";
  private static final String TEST_WRITE_OBJECT = "test-write-object";
  private static final String TEST_NON_EXISTENT_OBJECT = "non-existent-object";
  private static final String TEST_HNS_BUCKET = "hns-bucket";
  private static final String TEST_FLAT_BUCKET = "flat-bucket";
  private static final String TEST_NON_EXISTENT_BUCKET = "non-existent-bucket";
  private static final String TEST_FORBIDDEN_BUCKET = "forbidden-bucket";
  private static final String TEST_ERROR_BUCKET = "error-bucket";
  private static final String TEST_LOCATION = "US";
  private static final String TEST_FOLDER_NAME = "test-folder";
  private static final String TEST_DIR = "dir/";
  private static final String TEST_OBJECT_NAME = "test-object-name";
  private static final String BLOB_WRITE_SESSION_CONFIG_FIELD = "blobWriteSessionConfig";
  private static final int MB = 1024 * 1024;
  private static final String RAPID_STORAGE_CLASS = "RAPID";
  private static final String TEST_ENCRYPTION_KEY = "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=";
  private static final String TEST_DIRECTORY_CONTENT_TYPE = "application/x-directory";
  private static final String TEST_CONTENT_ENCODING = "identity";
  private static final long TEST_CONTENT_GENERATION = 123L;

  private static final GcsClientOptions TEST_GCS_CLIENT_OPTIONS =
      GcsClientOptions.builder().setProjectId(TEST_PROJECT).build();
  private static final GcsItemId TEST_ITEM_ID =
      GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
  private static final GcsItemId TEST_DIR_ITEM_ID =
      GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_DIR).build();
  private static final BlobInfo TEST_BLOB_INFO =
      BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT))
          .setContentType("application/octet-stream")
          .build();
  private static final GcsWriteOptions DEFAULT_WRITE_OPTIONS = GcsWriteOptions.builder().build();

  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final Supplier<ExecutorService> executorServiceSupplier =
      Suppliers.memoize(() -> Executors.newFixedThreadPool(30));
  private final Telemetry telemetry = new Telemetry(ImmutableList.of());

  private GcsClientImpl gcsClient;
  private Storage mockStorage;
  private StorageControlClient mockControlClient;
  private Bucket mockBucket;
  private Blob mockBlob;
  private GcsClientImpl clientWithMock;

  @BeforeEach
  void setUp() throws IOException {
    gcsClient = createClientWithMockStorage(storage);
    mockStorage = mock(Storage.class);
    mockControlClient = mock(StorageControlClient.class);
    mockBucket = mock(Bucket.class);
    mockBlob = mock(Blob.class);
    clientWithMock = createClientWithMocks(mockStorage, mockControlClient);
  }

  @Test
  void getGcsItemInfo_itemIdPointsToDirectory_throwsUnsupportedOperationException() {
    GcsItemId directoryItemId = GcsItemId.builder().setBucketName(TEST_BUCKET_ID).build();

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> gcsClient.getGcsItemInfo(directoryItemId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(String.format("Expected gcs object but got %s", directoryItemId));
  }

  @Test
  void getGcsItemInfo_gcsObjectExists_returnsItemInfo() throws IOException {
    String objectData = "hello world";
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_ID).setObjectName(TEST_OBJECT_ID).build();
    StorageTestUtils.createBlobInStorage(
        storage, BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);

    GcsItemInfo itemInfo = gcsClient.getGcsItemInfo(itemId);

    GcsItemId expectedItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_ID)
            .setObjectName(TEST_OBJECT_ID)
            .setContentGeneration(itemInfo.getContentGeneration().get())
            .build();
    assertThat(itemInfo.getItemId()).isEqualTo(expectedItemId);
    assertThat(itemInfo.getSize()).isEqualTo(objectData.length());
    assertThat(itemInfo.getContentGeneration().get()).isEqualTo(0L);
    assertThat(itemInfo.getVerificationAttributes().get().getMd5hash()).isNull();
    assertThat(itemInfo.getVerificationAttributes().get().getCrc32c()).isNotNull();
  }

  @Test
  void getGcsItemInfo_placeholderDirectory_returnsPlaceholderDirectoryItemInfo()
      throws IOException {
    GcsItemId directoryId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.getBucket()).thenReturn(TEST_BUCKET);
    when(mockBlob.getName()).thenReturn("dir/");
    when(mockBlob.getGeneration()).thenReturn(1L);
    when(mockBlob.isDirectory()).thenReturn(true);
    Storage mockStorage = mock(Storage.class);
    when(mockStorage.get(eq(BlobId.of(TEST_BUCKET, "dir/")), any(Storage.BlobGetOption[].class)))
        .thenReturn(mockBlob);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);

    GcsItemInfo itemInfo = clientWithMock.getGcsItemInfo(directoryId);

    assertThat(itemInfo.getItemId().getBucketName()).isEqualTo(TEST_BUCKET);
    assertThat(itemInfo.getItemId().getObjectName()).hasValue("dir/");
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
  }

  @Test
  void getGcsItemInfo_storageReturnsNull_returnsNotFoundItemInfo() throws IOException {
    GcsItemId nonExistentItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_NAME)
            .setObjectName(TEST_NON_EXISTENT_OBJECT)
            .build();

    GcsItemInfo itemInfo = gcsClient.getGcsItemInfo(nonExistentItemId);

    assertNotFound(itemInfo, nonExistentItemId);
  }

  @Test
  void openReadChannel_gcsObjectExists_returnsChannelWithCorrectSizeAndContent()
      throws IOException {
    String objectData = "hello world";
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(itemId)
            .setSize(objectData.length())
            .setContentGeneration(0L)
            .build();
    StorageTestUtils.createBlobInStorage(
        storage, BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    ByteBuffer buffer = ByteBuffer.allocate(objectData.length());

    SeekableByteChannel channel = gcsClient.openReadChannel(itemInfo, readOptions);
    int bytesRead = channel.read(buffer);

    assertThat(channel.size()).isEqualTo(objectData.length());
    assertThat(bytesRead).isEqualTo(objectData.length());
    assertThat(new String(buffer.array(), UTF_8)).isEqualTo(objectData);
  }

  @Test
  void openReadChannel_itemId_gcsObjectExists_returnsChannelWithCorrectSizeAndContent()
      throws IOException {
    String objectData = "hello world";
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    StorageTestUtils.createBlobInStorage(
        storage, BlobId.of(itemId.getBucketName(), itemId.getObjectName().get(), 0L), objectData);
    ByteBuffer buffer = ByteBuffer.allocate(objectData.length());

    SeekableByteChannel channel = gcsClient.openReadChannel(itemId, readOptions);
    int bytesRead = channel.read(buffer);

    assertThat(channel.size()).isEqualTo(objectData.length());
    assertThat(channel.size())
        .isEqualTo(objectData.length()); // Call twice to cover cached size branch
    assertThat(bytesRead).isEqualTo(objectData.length());
    assertThat(new String(buffer.array(), UTF_8)).isEqualTo(objectData);
  }

  @Test
  void openReadChannel_nullItemId_throwsNullPointerException() {
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> gcsClient.openReadChannel((GcsItemId) null, readOptions));
    assertThat(e).hasMessageThat().isEqualTo("gcsItemId should not be null");
  }

  @Test
  void openReadChannel_nullItemInfo_throwsNullPointerException() {
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();

    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> gcsClient.openReadChannel((GcsItemInfo) null, readOptions));
    assertThat(e).hasMessageThat().isEqualTo("itemInfo should not be null");
  }

  @Test
  void openReadChannel_nullReadOptions_throwsNullPointerException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT).build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(0L).setContentGeneration(0L).build();

    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsClient.openReadChannel(itemInfo, null));
    assertThat(e).hasMessageThat().isEqualTo("readOptions should not be null");
  }

  @Test
  void openReadChannel_itemInfoPointsToDirectory_throwsIllegalArgumentException() {
    GcsItemId directoryItemId = GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).build();
    GcsItemInfo directoryItemInfo =
        GcsItemInfo.builder()
            .setItemId(directoryItemId)
            .setSize(0L)
            .setContentGeneration(-1L)
            .build();
    GcsReadOptions readOptions = GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> gcsClient.openReadChannel(directoryItemInfo, readOptions));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected GCS object to be provided. But got: " + directoryItemId);
  }

  @Test
  void getUserAgent_noOptionalUserAgent() throws Exception {
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);

    String userAgent = client.getUserAgent();

    assertThat(userAgent).isEqualTo("gcs-analytics-core/" + VersionHelper.VERSION);
  }

  @Test
  void getUserAgent_withOptionalUserAgent() throws Exception {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId(TEST_PROJECT)
            .setUserAgent("custom-app/1.0")
            .build();
    GcsClientImpl client = new GcsClientImpl(options, executorServiceSupplier, telemetry);

    String userAgent = client.getUserAgent();

    assertThat(userAgent)
        .isEqualTo("gcs-analytics-core/" + VersionHelper.VERSION + " custom-app/1.0");
  }

  @Test
  void createStorage_withCredentials_usesProvidedCredentials() throws IOException {
    Credentials credentials = NoCredentials.getInstance();

    GcsClientImpl client =
        new GcsClientImpl(credentials, TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry);

    assertThat(client.storage.getOptions().getCredentials()).isEqualTo(credentials);
  }

  @Test
  void getBucketProperties_nullBucketName_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(NullPointerException.class, () -> clientWithMock.getBucketProperties(null));

    assertThat(e).hasMessageThat().isEqualTo("bucketName cannot be null");
  }

  @Test
  void getBucketProperties_hnsBucket_returnsHnsEnabled() throws IOException {
    Bucket mockBucket = mockBucketWithHns(true);
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_HNS_BUCKET), any(BucketGetOption.class));

    BucketProperties bucketProperties = clientWithMock.getBucketProperties(TEST_HNS_BUCKET);

    assertThat(bucketProperties.isHnsEnabled()).isTrue();
    assertThat(clientWithMock.isHnsBucket(TEST_HNS_BUCKET)).isTrue();
  }

  @Test
  void getBucketProperties_flatBucket_returnsDisabledHnsAndRapid() throws IOException {
    Bucket mockBucket = mockBucketWithHns(false);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.STANDARD);
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_FLAT_BUCKET), any(BucketGetOption.class));

    BucketProperties bucketProperties = clientWithMock.getBucketProperties(TEST_FLAT_BUCKET);

    assertThat(bucketProperties.isHnsEnabled()).isFalse();
    assertThat(bucketProperties.isRapid()).isFalse();
    assertThat(clientWithMock.isHnsBucket(TEST_FLAT_BUCKET)).isFalse();
  }

  @Test
  void getBucketProperties_rapidBucket_returnsRapidEnabled() throws IOException {
    Bucket mockBucket = mockBucketWithHns(true);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET), any(BucketGetOption.class));

    BucketProperties bucketProperties = clientWithMock.getBucketProperties(TEST_BUCKET);

    assertThat(bucketProperties.isHnsEnabled()).isTrue();
    assertThat(bucketProperties.isRapid()).isTrue();
  }

  @Test
  void getBucketProperties_missingHnsProperty_returnsDisabledHns() throws IOException {
    Bucket mockBucket = mockBucketWithHns(null);
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET), any(BucketGetOption.class));

    BucketProperties bucketProperties = clientWithMock.getBucketProperties(TEST_BUCKET);

    assertThat(bucketProperties.isHnsEnabled()).isFalse();
  }

  @Test
  void getBucketProperties_bucketNotFound_returnsDisabledHnsAndRapid() throws Exception {
    doReturn(null).when(mockStorage).get(eq(TEST_NON_EXISTENT_BUCKET), any(BucketGetOption.class));

    BucketProperties properties = clientWithMock.getBucketProperties(TEST_NON_EXISTENT_BUCKET);

    assertThat(properties.isHnsEnabled()).isFalse();
    assertThat(properties.isRapid()).isFalse();
  }

  @Test
  void getBucketProperties_forbiddenAccess_returnsDisabledHnsAndRapid() throws Exception {
    doThrow(new StorageException(403, "Forbidden"))
        .when(mockStorage)
        .get(eq(TEST_FORBIDDEN_BUCKET), any(BucketGetOption.class));

    BucketProperties properties = clientWithMock.getBucketProperties(TEST_FORBIDDEN_BUCKET);

    assertThat(properties.isHnsEnabled()).isFalse();
    assertThat(properties.isRapid()).isFalse();
  }

  @Test
  void getBucketProperties_storageThrows500_throwsIOException() {
    doThrow(new StorageException(500, "Internal Error"))
        .when(mockStorage)
        .get(eq(TEST_ERROR_BUCKET), any(BucketGetOption.class));

    IOException e =
        assertThrows(
            IOException.class, () -> clientWithMock.getBucketProperties(TEST_ERROR_BUCKET));

    assertThat(e).hasMessageThat().contains("Unable to access bucket: " + TEST_ERROR_BUCKET);
  }

  private GcsClientImpl createClientWithMockStorage(Storage mockStorage) {
    return new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry) {
      @Override
      protected Storage createStorage(Optional<Credentials> credentials) {
        return mockStorage;
      }

      @Override
      protected Storage createGrpcStorage(Optional<Credentials> credentials) {
        return mockStorage;
      }
    };
  }

  private Bucket mockBucketWithHns(Boolean hnsEnabled) {
    Bucket mockBucket = mock(Bucket.class);
    if (hnsEnabled != null) {
      HierarchicalNamespace hns = mock(HierarchicalNamespace.class);
      when(hns.getEnabled()).thenReturn(hnsEnabled);
      when(mockBucket.getHierarchicalNamespace()).thenReturn(hns);
    } else {
      when(mockBucket.getHierarchicalNamespace()).thenReturn(null);
    }
    return mockBucket;
  }

  @Test
  void create_withLocalStorage_writesSuccessfully() throws Exception {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_WRITE_OBJECT).build();
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_WRITE_OBJECT)).build();
    byte[] data = "hello write world".getBytes(StandardCharsets.UTF_8);

    try (WritableByteChannel channel =
        gcsClient.createWriteChannel(itemId, DEFAULT_WRITE_OPTIONS)) {
      int bytesWritten = channel.write(ByteBuffer.wrap(data));
      assertThat(bytesWritten).isEqualTo(data.length);
    }

    assertThat(new String(storage.readAllBytes(blobInfo.getBlobId()), StandardCharsets.UTF_8))
        .isEqualTo("hello write world");
  }

  @Test
  void create_whenAccessDenied_throwsAccessDeniedException() throws Exception {
    StorageException e403 = new StorageException(403, "Forbidden");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e403);

    assertThrows(
        AccessDeniedException.class,
        () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));
  }

  @Test
  void create_whenFileExists_throwsIOException() throws Exception {
    StorageException e409 = new StorageException(409, "Conflict");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e409);

    assertThrows(
        IOException.class,
        () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));
  }

  @Test
  void create_whenPreconditionFailedAndNoOverwrite_throwsFileAlreadyExistsException()
      throws Exception {
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setOverwriteExisting(false).build();
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    FileAlreadyExistsException exception =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, writeOptions));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenGenerationMismatch_throwsIOException() throws Exception {
    GcsItemId itemIdWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_OBJECT)
            .setContentGeneration(12345L)
            .build();
    BlobInfo blobInfoWithGen =
        BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT, 12345L))
            .setContentType("application/octet-stream")
            .build();
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(blobInfoWithGen), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    IOException exception =
        assertThrows(
            IOException.class,
            () -> clientWithMock.createWriteChannel(itemIdWithGen, DEFAULT_WRITE_OPTIONS));

    assertThat(exception).hasMessageThat().contains("Generation mismatch for object");
    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenBucketOrObjectNotFound_throwsFileNotFoundException() throws Exception {
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName(TEST_NON_EXISTENT_BUCKET)
            .setObjectName(TEST_NON_EXISTENT_OBJECT)
            .build();
    BlobInfo blobInfo =
        BlobInfo.newBuilder(BlobId.of(TEST_NON_EXISTENT_BUCKET, TEST_NON_EXISTENT_OBJECT))
            .setContentType("application/octet-stream")
            .build();
    StorageException e404 = new StorageException(404, "Not Found");
    when(mockStorage.blobWriteSession(eq(blobInfo), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e404);

    FileNotFoundException exception =
        assertThrows(
            FileNotFoundException.class,
            () -> clientWithMock.createWriteChannel(itemId, DEFAULT_WRITE_OPTIONS));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e404);
  }

  @Test
  void create_whenStorageExceptionOccurs_throwsIOException() throws Exception {
    StorageException e500 = new StorageException(500, "Internal Server Error");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e500);

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));

    assertThat(thrown).hasMessageThat().contains("Error during initialization to GCS");
  }

  @Test
  void create_whenOpenThrowsIOExceptionWrappingStorageException_translatesStorageException()
      throws Exception {
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    StorageException nestedStorageException = new StorageException(404, "Not Found");
    IOException wrappingException = new IOException(nestedStorageException);
    when(mockSession.open()).thenThrow(wrappingException);

    IOException exception =
        assertThrows(
            IOException.class, () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, null));

    assertThat(exception).isInstanceOf(FileNotFoundException.class);
    assertThat(exception).hasCauseThat().isSameInstanceAs(nestedStorageException);
  }

  @Test
  void create_nullWriteOptions_usesDefaultWriteOptions() throws Exception {
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    WritableByteChannel mockChannel = mock(WritableByteChannel.class);
    when(mockSession.open()).thenReturn(mockChannel);

    WritableByteChannel returnedChannel = clientWithMock.createWriteChannel(TEST_ITEM_ID, null);

    assertThat(returnedChannel).isInstanceOf(GcsWriteChannel.class);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    when(mockChannel.isOpen()).thenReturn(true);
    returnedChannel.write(buffer);
    verify(mockChannel).write(buffer);
  }

  @Test
  void create_withMetadata_base64EncodesMetadata() throws Exception {
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    Map<String, byte[]> customMetadata = new HashMap<>();
    customMetadata.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
    customMetadata.put("key2", new byte[] {0, 1, 2, 3});

    GcsWriteOptions options =
        GcsWriteOptions.builder()
            .setMetadata(customMetadata)
            .setContentType("text/plain")
            .setContentEncoding("gzip")
            .build();
    ArgumentCaptor<BlobInfo> blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);

    clientWithMock.createWriteChannel(TEST_ITEM_ID, options);

    verify(mockStorage).blobWriteSession(blobInfoCaptor.capture(), any());
    BlobInfo capturedBlobInfo = blobInfoCaptor.getValue();
    assertThat(capturedBlobInfo.getMetadata()).containsEntry("key1", "dmFsdWUx");
    assertThat(capturedBlobInfo.getMetadata()).containsEntry("key2", "AAECAw==");
    assertThat(capturedBlobInfo.getContentType()).isEqualTo("text/plain");
    assertThat(capturedBlobInfo.getContentEncoding()).isEqualTo("gzip");
  }

  @Test
  void create_withoutObjectName_throwsIllegalArgumentException() {
    GcsItemId itemIdWithoutName = GcsItemId.builder().setBucketName(TEST_BUCKET).build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> clientWithMock.createWriteChannel(itemIdWithoutName, DEFAULT_WRITE_OPTIONS));
    assertThat(e).hasMessageThat().contains("Object name must be present");
  }

  @Test
  void create_allWriteOptionsEnabled_generatesCorrectBlobWriteOptions() throws Exception {
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsWriteOptions allOptions =
        GcsWriteOptions.builder()
            .setChecksumValidationEnabled(true)
            .setDisableGzipContent(true)
            .setOverwriteExisting(false)
            .setKmsKeyName("kms-key")
            .setEncryptionKey("MDEyMzQ1Njc4OUFCQ0RFRkdISUpLTE1OT1BRUlNUVVU=")
            .setUserProject("user-project")
            .build();

    clientWithMock.createWriteChannel(TEST_ITEM_ID, allOptions);

    String capturedOptionsString = captureBlobWriteOptions(mockStorage, TEST_BLOB_INFO);
    assertThat(capturedOptionsString).contains("Crc32cMatchExtractor");
    assertThat(capturedOptionsString).contains("IF_GENERATION_MATCH");
    assertThat(capturedOptionsString).contains("KMS_KEY_NAME");
    assertThat(capturedOptionsString).contains("CUSTOMER_SUPPLIED_KEY");
  }

  @Test
  void create_withDisableGzipContentFalse_doesNotAddDisableGzipOption() throws Exception {
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setDisableGzipContent(false).build();

    clientWithMock.createWriteChannel(TEST_ITEM_ID, writeOptions);

    String capturedOptionsString = captureBlobWriteOptions(mockStorage, TEST_BLOB_INFO);
    assertThat(capturedOptionsString).doesNotContain("disableGzipContent");
  }

  @Test
  void create_withGenerationId_generatesGenerationMatchOption() throws Exception {
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    when(mockSession.open()).thenReturn(mock(WritableByteChannel.class));
    GcsItemId itemIdWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_OBJECT)
            .setContentGeneration(12345L)
            .build();
    BlobInfo blobInfoWithGen =
        BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT, 12345L))
            .setContentType("application/octet-stream")
            .build();

    clientWithMock.createWriteChannel(itemIdWithGen, DEFAULT_WRITE_OPTIONS);

    String capturedOptionsString = captureBlobWriteOptions(mockStorage, blobInfoWithGen);
    assertThat(capturedOptionsString)
        .contains("GenerationMatch{key=IF_GENERATION_MATCH, val=12345}");
  }

  @ParameterizedTest
  @EnumSource(GcsClientOptions.PartFileCleanupType.class)
  void createStorage_withParallelCompositeUpload_setsPcuSessionConfig(
      GcsClientOptions.PartFileCleanupType cleanupType) throws Exception {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
            .setPcuBufferCount(5)
            .setPcuBufferCapacity(128 * MB)
            .setPcuPartFileCleanupType(cleanupType)
            .setPcuPartFileNamePrefix("custom-prefix-")
            .build();

    assertPcuSessionConfig(clientOptions);
  }

  @Test
  void createStorage_withWriteToDiskThenUpload_setsBufferToDiskSessionConfig() throws Exception {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD)
            .setTemporaryPaths(ImmutableList.of("/tmp/path1"))
            .build();

    GcsClientImpl client = createClientWithClientOptions(clientOptions);

    assertThat(getBlobWriteSessionConfig(client.storage.getOptions()))
        .isInstanceOf(BufferToDiskThenUpload.class);
  }

  @Test
  void createStorage_whenBufferToTempDirThrowsIOException_throwsUncheckedIOException() {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD)
            .build();

    try (MockedStatic<BlobWriteSessionConfigs> mocked = mockStatic(BlobWriteSessionConfigs.class)) {
      mocked
          .when(BlobWriteSessionConfigs::bufferToTempDirThenUpload)
          .thenThrow(new IOException("mock error"));
      UncheckedIOException e =
          assertThrows(
              UncheckedIOException.class, () -> createClientWithClientOptions(clientOptions));
      assertThat(e).hasMessageThat().contains("Failed while initializing configs");
    }
  }

  @Test
  void createStorage_withJournalingOnHttp_throwsUnsupportedOperationException() {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.JOURNALING)
            .setTemporaryPaths(ImmutableList.of("/tmp/path1"))
            .build();

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class,
            () -> createClientWithClientOptions(clientOptions));

    assertThat(e).hasMessageThat().contains("JOURNALING upload type is not supported");
  }

  @Test
  void getGcsItemInfo_storageThrows500_throwsIOException() throws Exception {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl clientWithMock = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption[].class)))
        .thenThrow(new StorageException(500, "Internal Server Error"));

    IOException e = assertThrows(IOException.class, () -> clientWithMock.getGcsItemInfo(itemId));

    assertThat(e).hasMessageThat().contains("Unable to access blob");
  }

  @Test
  void create_whenOpenThrowsIOException_propagatesIOException() throws Exception {
    BlobWriteSession mockSession = mockBlobWriteSession(mockStorage);
    IOException ioException = new IOException("Open failed");
    when(mockSession.open()).thenThrow(ioException);

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));

    assertThat(thrown).isSameInstanceAs(ioException);
  }

  @Test
  void createStorage_withWriteToDiskAndNoTempPaths_setsBufferToTempDirSessionConfig()
      throws Exception {
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD)
            .setTemporaryPaths(ImmutableList.of()) // Empty paths
            .build();

    GcsClientImpl client = createClientWithClientOptions(clientOptions);

    assertThat(getBlobWriteSessionConfig(client.storage.getOptions()))
        .isInstanceOf(BufferToDiskThenUpload.class);
  }

  @Test
  void createStorage_withChunkUpload_setsDefaultSessionConfig() throws Exception {
    int customChunkSize = 2 * MB;
    GcsClientOptions clientOptions =
        GcsClientOptions.builder()
            .setUploadType(GcsClientOptions.UploadType.CHUNK_UPLOAD)
            .setUploadChunkSize(customChunkSize)
            .build();

    GcsClientImpl client = createClientWithClientOptions(clientOptions);

    BlobWriteSessionConfig config = getBlobWriteSessionConfig(client.storage.getOptions());
    assertThat(config).isInstanceOf(DefaultBlobWriteSessionConfig.class);
    assertThat(((DefaultBlobWriteSessionConfig) config).getChunkSize()).isEqualTo(customChunkSize);
  }

  @Test
  void create_whenPreconditionFailedAndNullWriteOptions_throwsIOException() throws Exception {
    StorageException e412 = new StorageException(412, "Precondition Failed");
    BlobInfo nullOptionsBlobInfo = BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT)).build();
    when(mockStorage.blobWriteSession(
            eq(nullOptionsBlobInfo), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    IOException exception =
        assertThrows(
            IOException.class, () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, null));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenPreconditionFailedAndDefaultOverwrite_throwsIOException() throws Exception {
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().setOverwriteExisting(true).build();
    StorageException e412 = new StorageException(412, "Precondition Failed");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(e412);

    IOException exception =
        assertThrows(
            IOException.class, () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, writeOptions));

    assertThat(exception).hasCauseThat().isSameInstanceAs(e412);
  }

  @Test
  void create_whenRuntimeExceptionOccurs_directlyPropagates() throws Exception {
    RuntimeException runtimeException = new NullPointerException("mock null pointer exception");
    when(mockStorage.blobWriteSession(eq(TEST_BLOB_INFO), any(Storage.BlobWriteOption[].class)))
        .thenThrow(runtimeException);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> clientWithMock.createWriteChannel(TEST_ITEM_ID, DEFAULT_WRITE_OPTIONS));

    assertThat(exception).isSameInstanceAs(runtimeException);
  }

  private BlobWriteSessionConfig getBlobWriteSessionConfig(StorageOptions options) {
    Class<?> clazz = options.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(BLOB_WRITE_SESSION_CONFIG_FIELD);
        field.setAccessible(true);
        return (BlobWriteSessionConfig) field.get(options);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException(
        new NoSuchFieldException(
            "Field " + BLOB_WRITE_SESSION_CONFIG_FIELD + " not found in options hierarchy"));
  }

  private void assertPcuSessionConfig(GcsClientOptions clientOptions) throws Exception {
    GcsClientImpl client = createClientWithClientOptions(clientOptions);

    assertThat(getBlobWriteSessionConfig(client.storage.getOptions()))
        .isInstanceOf(ParallelCompositeUploadBlobWriteSessionConfig.class);
  }

  private GcsClientImpl createClientWithClientOptions(GcsClientOptions clientOptions) {
    return new GcsClientImpl(clientOptions, executorServiceSupplier, telemetry);
  }

  private BlobWriteSession mockBlobWriteSession(Storage mockStorage) {
    BlobWriteSession mockSession = mock(BlobWriteSession.class);
    when(mockStorage.blobWriteSession(any(BlobInfo.class), any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockSession);
    return mockSession;
  }

  private String captureBlobWriteOptions(Storage mockStorage, BlobInfo blobInfo) {
    ArgumentCaptor<Storage.BlobWriteOption[]> optionsCaptor =
        ArgumentCaptor.forClass(Storage.BlobWriteOption[].class);
    verify(mockStorage).blobWriteSession(eq(blobInfo), optionsCaptor.capture());
    return Arrays.toString(optionsCaptor.getValue());
  }

  @Test
  void createStorage_bidiDisabled_usesHttpTransport() throws IOException {
    GcsClientImpl client =
        new GcsClientImpl(
            NoCredentials.getInstance(),
            TEST_GCS_CLIENT_OPTIONS,
            executorServiceSupplier,
            telemetry);
    assertThat(client.storage.getOptions()).isInstanceOf(HttpStorageOptions.class);
  }

  @Test
  void createStorage_bidiEnabled_usesGrpcTransport() throws IOException {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId(TEST_PROJECT)
            .setGcsReadOptions(GcsReadOptions.builder().setBidiReadEnabled(true).build())
            .build();

    GcsClientImpl client =
        new GcsClientImpl(NoCredentials.getInstance(), options, executorServiceSupplier, telemetry);

    assertThat(client.storage.getOptions()).isInstanceOf(GrpcStorageOptions.class);
  }

  @Test
  void openReadChannel_bidiEnabled_returnsGcsBidiReadChannel() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).setBidiReadEnabled(true).build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    GcsItemInfo itemInfo =
        GcsItemInfo.builder().setItemId(itemId).setSize(100L).setContentGeneration(0L).build();
    Storage mockStorage = mock(Storage.class);
    ApiFuture<BlobReadSession> mockSessionFuture = mock(ApiFuture.class);
    when(mockStorage.blobReadSession(any(BlobId.class))).thenReturn(mockSessionFuture);
    GcsClient bidiClient = createClientWithMockStorage(mockStorage);

    VectoredSeekableByteChannel channel = bidiClient.openReadChannel(itemInfo, readOptions);

    assertThat(channel).isInstanceOf(GcsBidiReadChannel.class);
  }

  @Test
  void openReadChannel_itemId_bidiEnabled_returnsGcsBidiReadChannel() throws IOException {
    GcsReadOptions readOptions =
        GcsReadOptions.builder().setUserProjectId(TEST_PROJECT).setBidiReadEnabled(true).build();
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_NAME).build();
    Storage mockStorage = mock(Storage.class);
    ApiFuture<BlobReadSession> mockSessionFuture = mock(ApiFuture.class);
    when(mockStorage.blobReadSession(any(BlobId.class))).thenReturn(mockSessionFuture);
    GcsClient bidiClient = createClientWithMockStorage(mockStorage);

    VectoredSeekableByteChannel channel = bidiClient.openReadChannel(itemId, readOptions);

    assertThat(channel).isInstanceOf(GcsBidiReadChannel.class);
  }

  @Test
  void getBucketInfo_nullItemId_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsClient.getBucketInfo(null));

    assertThat(e).hasMessageThat().contains("Item ID must not be null");
  }

  @Test
  void getBucketInfo_notBucketItemId_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> gcsClient.getBucketInfo(TEST_ITEM_ID));

    assertThat(e).hasMessageThat().contains("Expected a bucket itemId");
  }

  @Test
  void getBucketInfo_storageReturnsNull_returnsNotFoundItemInfo() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_NON_EXISTENT_BUCKET).build();
    when(mockStorage.get(eq(TEST_NON_EXISTENT_BUCKET), any(Storage.BucketGetOption[].class)))
        .thenReturn(null);

    GcsItemInfo itemInfo = clientWithMock.getBucketInfo(bucketId);

    assertNotFound(itemInfo, bucketId);
  }

  @Test
  void getBucketInfo_bucketExists_returnsBucketInfo() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    Bucket mockBucket = createMockBucket(TEST_BUCKET);
    when(mockStorage.get(eq(TEST_BUCKET), any(Storage.BucketGetOption[].class)))
        .thenReturn(mockBucket);

    GcsItemInfo itemInfo = clientWithMock.getBucketInfo(bucketId);

    assertThat(itemInfo.getItemId()).isEqualTo(bucketId);
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
    assertThat(itemInfo.getSize()).isEqualTo(0L);
    assertThat(itemInfo.getLocation()).hasValue(TEST_LOCATION);
    assertThat(itemInfo.getMetaGeneration()).isEqualTo(2L);
    assertThat(itemInfo.getCreationTime())
        .isEqualTo(OffsetDateTime.parse("2026-08-01T10:00:00Z").toInstant().toEpochMilli());
    assertThat(itemInfo.getModificationTime())
        .isEqualTo(OffsetDateTime.parse("2026-08-02T10:00:00Z").toInstant().toEpochMilli());
  }

  @Test
  void getBucketInfo_storageThrows404_returnsNotFoundItemInfo() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    when(mockStorage.get(eq(TEST_BUCKET), any(Storage.BucketGetOption[].class)))
        .thenThrow(new StorageException(404, "Not Found"));

    GcsItemInfo itemInfo = clientWithMock.getBucketInfo(bucketId);

    assertNotFound(itemInfo, bucketId);
  }

  @Test
  void getBucketInfo_storageThrows500_throwsIOException() {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    when(mockStorage.get(eq(TEST_BUCKET), any(Storage.BucketGetOption[].class)))
        .thenThrow(new StorageException(500, "Internal Error"));

    IOException e = assertThrows(IOException.class, () -> clientWithMock.getBucketInfo(bucketId));

    assertThat(e).hasMessageThat().contains("Unable to access bucket: " + TEST_BUCKET);
  }

  @Test
  void getFolderInfo_nullItemId_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsClient.getFolderInfo(null));

    assertThat(e).hasMessageThat().contains("Item ID must not be null");
  }

  @Test
  void getFolderInfo_rootItemId_returnsRootInfo() throws IOException {
    GcsItemInfo itemInfo = gcsClient.getFolderInfo(GcsItemId.ROOT);

    assertThat(itemInfo).isEqualTo(GcsItemInfo.ROOT_INFO);
  }

  @Test
  void getFolderInfo_bucketItemId_returnsBucketInfo() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    Bucket mockBucket = createMockBucket(TEST_BUCKET);
    when(mockStorage.get(eq(TEST_BUCKET), any(Storage.BucketGetOption[].class)))
        .thenReturn(mockBucket);

    GcsItemInfo itemInfo = clientWithMock.getFolderInfo(bucketId);

    assertThat(itemInfo.getItemId()).isEqualTo(bucketId);
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
  }

  @Test
  void getFolderInfo_emptyFolderName_throwsIllegalArgumentException() {
    GcsItemId rootSlashId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("/").build();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> gcsClient.getFolderInfo(rootSlashId));

    assertThat(e).hasMessageThat().contains("Folder name cannot be empty");
  }

  @Test
  void getFolderInfo_folderExists_returnsFolderInfo() throws IOException {
    GcsItemId folderItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_FOLDER_NAME + "/")
            .build();
    Folder mockFolder = createMockFolder(TEST_BUCKET, TEST_FOLDER_NAME);
    when(mockControlClient.getFolder(any(GetFolderRequest.class))).thenReturn(mockFolder);

    GcsItemInfo itemInfo = clientWithMock.getFolderInfo(folderItemId);

    assertThat(itemInfo.getItemId()).isEqualTo(folderItemId);
    assertThat(itemInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
    assertThat(itemInfo.getSize()).isEqualTo(0L);
    assertThat(itemInfo.getMetaGeneration()).isEqualTo(3L);
    assertThat(itemInfo.getCreationTime())
        .isEqualTo(Instant.ofEpochSecond(1000, 500).toEpochMilli());
    assertThat(itemInfo.getModificationTime())
        .isEqualTo(Instant.ofEpochSecond(2000, 500).toEpochMilli());
  }

  @Test
  void getFolderInfo_folderWithoutCreateOrUpdateTime_setsTimestampsToZero() throws IOException {
    GcsItemId folderItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_FOLDER_NAME + "/")
            .build();
    when(mockControlClient.getFolder(any(GetFolderRequest.class)))
        .thenReturn(Folder.getDefaultInstance());

    GcsItemInfo itemInfo = clientWithMock.getFolderInfo(folderItemId);

    assertThat(itemInfo.getCreationTime()).isEqualTo(0L);
    assertThat(itemInfo.getModificationTime()).isEqualTo(0L);
  }

  @Test
  void createBucket_success() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage);

    client.createBucket(TEST_BUCKET_NAME);

    verify(mockStorage).create(BucketInfo.of(TEST_BUCKET_NAME));
  }

  @Test
  void createBucket_alreadyExists_throwsFileAlreadyExistsException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage);
    when(mockStorage.create(any(BucketInfo.class)))
        .thenThrow(new StorageException(409, "Bucket already exists"));

    assertThrows(FileAlreadyExistsException.class, () -> client.createBucket(TEST_BUCKET_NAME));
  }

  @Test
  void createBucket_storageException_throwsIOException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage);
    when(mockStorage.create(any(BucketInfo.class)))
        .thenThrow(new StorageException(500, "Internal Server Error"));

    assertThrows(IOException.class, () -> client.createBucket(TEST_BUCKET_NAME));
  }

  @Test
  void createBucket_nullOrEmptyBucketName_throwsIllegalArgumentException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage);

    assertThrows(NullPointerException.class, () -> client.createBucket(null));
    assertThrows(IllegalArgumentException.class, () -> client.createBucket(""));
  }

  @Test
  void createEmptyObject_success() throws IOException {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();

    client.createEmptyObject(itemId);

    verify(mockStorage)
        .create(
            eq(BlobInfo.newBuilder(BlobId.of(TEST_BUCKET_NAME, "dir/")).build()),
            eq(new byte[0]),
            any(Storage.BlobTargetOption.class));
  }

  @Test
  void createEmptyObject_alreadyExists_throwsFileAlreadyExistsException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    when(mockStorage.create(
            any(BlobInfo.class), eq(new byte[0]), any(Storage.BlobTargetOption.class)))
        .thenThrow(new StorageException(409, "Object already exists"));

    assertThrows(FileAlreadyExistsException.class, () -> client.createEmptyObject(itemId));
  }

  @Test
  void createEmptyObject_storageException_throwsIOException() {
    Storage mockStorage = mock(Storage.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    when(mockStorage.create(
            any(BlobInfo.class), eq(new byte[0]), any(Storage.BlobTargetOption.class)))
        .thenThrow(new StorageException(500, "Internal Server Error"));

    assertThrows(IOException.class, () -> client.createEmptyObject(itemId));
  }

  @Test
  void createFolder_success() throws IOException {
    Storage mockStorage = mock(Storage.class);
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage);
    client.storageControlClient = mockControlClient;
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    CreateFolderRequest expectedRequest =
        CreateFolderRequest.newBuilder()
            .setParent("projects/_/buckets/" + TEST_BUCKET_NAME)
            .setFolderId("dir")
            .setRecursive(true)
            .build();

    client.createFolder(itemId, true);

    verify(mockControlClient).createFolder(expectedRequest);
  }

  @Test
  void createFolder_alreadyExists_throwsFileAlreadyExistsException() {
    Storage mockStorage = mock(Storage.class);
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage);
    client.storageControlClient = mockControlClient;
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    AlreadyExistsException alreadyExistsException = mock(AlreadyExistsException.class);
    when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
        .thenThrow(alreadyExistsException);

    assertThrows(FileAlreadyExistsException.class, () -> client.createFolder(itemId, true));
  }

  @Test
  void createFolder_runtimeException_throwsIOException() {
    Storage mockStorage = mock(Storage.class);
    StorageControlClient mockControlClient = mock(StorageControlClient.class);
    GcsClientImpl client = createClientWithMockStorage(mockStorage);
    client.storageControlClient = mockControlClient;
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
        .thenThrow(new RuntimeException("RPC failure"));

    assertThrows(IOException.class, () -> client.createFolder(itemId, true));
  }

  @Test
  void createBucket_success() throws IOException {
    clientWithMock.createBucket(TEST_BUCKET_NAME);
    verify(mockStorage).create(BucketInfo.of(TEST_BUCKET_NAME));
  }

  @Test
  void createBucket_alreadyExists_throwsFileAlreadyExistsException() {
    when(mockStorage.create(any(BucketInfo.class)))
        .thenThrow(new StorageException(409, "Bucket already exists"));
    assertThrows(
        FileAlreadyExistsException.class, () -> clientWithMock.createBucket(TEST_BUCKET_NAME));
  }

  @Test
  void createBucket_storageException_throwsIOException() {
    when(mockStorage.create(any(BucketInfo.class)))
        .thenThrow(new StorageException(500, "Internal Server Error"));
    assertThrows(IOException.class, () -> clientWithMock.createBucket(TEST_BUCKET_NAME));
  }

  @Test
  void createBucket_nullOrEmptyBucketName_throwsIllegalArgumentException() {
    assertThrows(NullPointerException.class, () -> clientWithMock.createBucket(null));
    assertThrows(IllegalArgumentException.class, () -> clientWithMock.createBucket(""));
  }

  @Test
  void createEmptyObject_success() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();

    clientWithMock.createEmptyObject(itemId);

    verify(mockStorage)
        .create(
            eq(
                BlobInfo.newBuilder(BlobId.of(TEST_BUCKET_NAME, "dir/"))
                    .setContentType("application/octet-stream")
                    .build()),
            eq(Storage.BlobTargetOption.disableGzipContent()),
            eq(Storage.BlobTargetOption.doesNotExist()));
  }

  @Test
  void createEmptyObject_alreadyExistsOrPreconditionFailed_throwsFileAlreadyExistsException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(409, "Object already exists"))
        .thenThrow(new StorageException(412, "Precondition Failed"));

    assertThrows(FileAlreadyExistsException.class, () -> clientWithMock.createEmptyObject(itemId));
    assertThrows(FileAlreadyExistsException.class, () -> clientWithMock.createEmptyObject(itemId));
  }

  @Test
  void createEmptyObject_storageException_throwsIOException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(500, "Internal Server Error"));

    assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(itemId));
  }

  @Test
  void createEmptyObject_withOptionsAndGeneration_setsBlobInfoAndTargetOptions()
      throws IOException {
    GcsItemId itemIdWithGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET_NAME)
            .setObjectName(TEST_DIR)
            .setContentGeneration(TEST_CONTENT_GENERATION)
            .build();
    GcsWriteOptions options =
        GcsWriteOptions.builder()
            .setDisableGzipContent(true)
            .setOverwriteExisting(true)
            .setContentType(TEST_DIRECTORY_CONTENT_TYPE)
            .setContentEncoding(TEST_CONTENT_ENCODING)
            .setMetadata(ImmutableMap.of("key", "val".getBytes(StandardCharsets.UTF_8)))
            .setEncryptionKey(TEST_ENCRYPTION_KEY)
            .build();
    GcsItemId itemIdObj =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName(TEST_OBJECT_ID).build();

    clientWithMock.createEmptyObject(itemIdWithGen, options);
    clientWithMock.createEmptyObject(itemIdObj, options);

    verify(mockStorage)
        .create(
            eq(
                BlobInfo.newBuilder(BlobId.of(TEST_BUCKET_NAME, TEST_DIR))
                    .setContentType(TEST_DIRECTORY_CONTENT_TYPE)
                    .setContentEncoding(TEST_CONTENT_ENCODING)
                    .setMetadata(ImmutableMap.of("key", "dmFs"))
                    .build()),
            eq(Storage.BlobTargetOption.disableGzipContent()),
            eq(Storage.BlobTargetOption.generationMatch(TEST_CONTENT_GENERATION)),
            eq(Storage.BlobTargetOption.encryptionKey(TEST_ENCRYPTION_KEY)));
    verify(mockStorage, times(2))
        .create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class));
  }

  @Test
  void createEmptyObject_ignoreExceptionWhenMarkerAlreadyExists_success() throws IOException {
    when(mockBlob.getBucket()).thenReturn(TEST_BUCKET_NAME);
    when(mockBlob.getName()).thenReturn("dir/");
    when(mockBlob.getSize()).thenReturn(0L);
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(429, "Too Many Requests"));
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption.class)))
        .thenReturn(mockBlob);

    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID);

    verify(mockStorage).create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class));
  }

  @Test
  void createEmptyObject_metadataMismatch_withEnsureMatchFalse_succeeds() throws IOException {
    when(mockBlob.getBucket()).thenReturn(TEST_BUCKET_NAME);
    when(mockBlob.getName()).thenReturn("dir/");
    when(mockBlob.getSize()).thenReturn(0L);
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(429, "Too Many Requests"));
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption.class)))
        .thenReturn(mockBlob);
    GcsWriteOptions options =
        GcsWriteOptions.builder()
            .setMetadata(ImmutableMap.of("key", new byte[] {1, 2, 3}))
            .setEnsureEmptyObjectsMetadataMatch(false)
            .build();

    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID, options);

    verify(mockStorage).create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class));
  }

  @Test
  void createEmptyObject_rapidStorageClass_usesAppendableUpload() throws IOException {
    BlobAppendableUpload mockUpload = mock(BlobAppendableUpload.class);
    AppendableUploadWriteableByteChannel mockChannel =
        mock(AppendableUploadWriteableByteChannel.class);
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockUpload);
    when(mockUpload.open()).thenReturn(mockChannel);

    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID);

    verify(mockChannel).write(any(java.nio.ByteBuffer.class));
    verify(mockChannel).finalizeAndClose();
  }

  @Test
  void createEmptyObject_transientReadErrorDuringVerification_retriesAndSucceeds()
      throws IOException {
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(429, "Too Many Requests"));
    when(mockBlob.getBucket()).thenReturn(TEST_BUCKET);
    when(mockBlob.getName()).thenReturn("dir/");
    when(mockBlob.getSize()).thenReturn(0L);
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class)))
        .thenThrow(new StorageException(503, "Service Unavailable"))
        .thenReturn(mockBlob);

    clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID);

    verify(mockStorage, times(2))
        .get(eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class));
  }

  @Test
  void
      createEmptyObject_nonRetryableReadErrorDuringVerification_throwsVerificationErrorWithContext() {
    StorageException originalException = new StorageException(429, "Rate limit exceeded");
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(originalException);
    when(mockStorage.get(
            eq(BlobId.of(TEST_BUCKET_NAME, TEST_DIR)), any(Storage.BlobGetOption[].class)))
        .thenThrow(new StorageException(403, "Permission Denied"));

    IOException e =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));

    assertThat(e).hasMessageThat().contains("Failed to verify existence of 0-byte object");
    assertThat(e).hasCauseThat().isEqualTo(originalException);
    assertThat(e.getCause().getSuppressed()).isNotEmpty();
  }

  @Test
  void
      createEmptyObject_interruptedDuringPolling_checksPredicateFinalTimeAndRestoresInterruptStatus() {
    when(mockStorage.create(any(BlobInfo.class), any(Storage.BlobTargetOption[].class)))
        .thenThrow(new StorageException(429, "Too Many Requests"));
    when(mockStorage.get(any(BlobId.class), any(Storage.BlobGetOption[].class))).thenReturn(null);
    Thread.currentThread().interrupt();

    IOException e =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));

    boolean wasInterrupted = Thread.interrupted();
    assertThat(wasInterrupted).isTrue();
    assertThat(e).hasMessageThat().contains("Failed to create empty object");
  }

  @Test
  void createEmptyObject_rapidStorageClass_throwsExceptionOnUploadError() throws IOException {
    when(mockBucket.getStorageClass()).thenReturn(StorageClass.valueOf(RAPID_STORAGE_CLASS));
    doReturn(mockBucket).when(mockStorage).get(eq(TEST_BUCKET_NAME), any(BucketGetOption.class));
    BlobAppendableUpload mockUpload = mock(BlobAppendableUpload.class);
    when(mockStorage.blobAppendableUpload(
            any(BlobInfo.class),
            any(BlobAppendableUploadConfig.class),
            any(Storage.BlobWriteOption[].class)))
        .thenReturn(mockUpload);
    StorageException storageEx = new StorageException(500, "Internal Error");
    IOException ioEx = new IOException("Socket closed");
    when(mockUpload.open()).thenThrow(new IOException(storageEx)).thenThrow(ioEx);

    IOException e1 =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));
    IOException e2 =
        assertThrows(IOException.class, () -> clientWithMock.createEmptyObject(TEST_DIR_ITEM_ID));

    assertThat(e1.getCause()).isEqualTo(storageEx);
    assertThat(e2).isEqualTo(ioEx);
  }

  @Test
  void createFolder_success() throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    CreateFolderRequest expectedRequest =
        CreateFolderRequest.newBuilder()
            .setParent("projects/_/buckets/" + TEST_BUCKET_NAME)
            .setFolderId("dir")
            .setRecursive(true)
            .build();

    clientWithMock.createFolder(itemId, true);

    verify(mockControlClient).createFolder(expectedRequest);
  }

  @Test
  void createFolder_emptyFolderName_throwsIllegalArgumentException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("/").build();

    assertThrows(IllegalArgumentException.class, () -> clientWithMock.createFolder(itemId, true));
  }

  @Test
  void createFolder_alreadyExists_throwsFileAlreadyExistsException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    AlreadyExistsException alreadyExistsException = mock(AlreadyExistsException.class);
    when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
        .thenThrow(alreadyExistsException);

    assertThrows(FileAlreadyExistsException.class, () -> clientWithMock.createFolder(itemId, true));
  }

  @Test
  void createFolder_runtimeException_throwsIOException() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET_NAME).setObjectName("dir/").build();
    when(mockControlClient.createFolder(any(CreateFolderRequest.class)))
        .thenThrow(new RuntimeException("RPC failure"));

    assertThrows(IOException.class, () -> clientWithMock.createFolder(itemId, true));
  }

  @Test
  void close_withAllClientsInitialized_closesAllAndNullsReferences() throws Exception {
    Storage mockGrpcStorage = mock(Storage.class);
    clientWithMock.storageControlClient = mockControlClient;
    clientWithMock.grpcStorage = mockGrpcStorage;

    clientWithMock.close();

    verify(mockStorage).close();
    verify(mockControlClient).close();
    verify(mockGrpcStorage).close();
    assertThat(clientWithMock.storageControlClient).isNull();
    assertThat(clientWithMock.grpcStorage).isNull();
  }

  @Test
  void close_whenUnderlyingClientsThrowException_suppressesException() throws Exception {
    Storage mockGrpcStorage = mock(Storage.class);
    doThrow(new RuntimeException("storage close error")).when(mockStorage).close();
    doThrow(new RuntimeException("grpc storage close error")).when(mockGrpcStorage).close();
    doThrow(new RuntimeException("control client close error")).when(mockControlClient).close();
    clientWithMock.storageControlClient = mockControlClient;
    clientWithMock.grpcStorage = mockGrpcStorage;

    clientWithMock.close();

    verify(mockStorage).close();
    verify(mockGrpcStorage).close();
    verify(mockControlClient).close();
    assertThat(clientWithMock.storageControlClient).isNull();
    assertThat(clientWithMock.grpcStorage).isNull();
  }

  @Test
  void createStorageControlClient_withCredentials_createsClient() throws Exception {
    try (StorageControlClient controlClient =
        gcsClient.createStorageControlClient(Optional.of(NoCredentials.getInstance()))) {
      assertThat(controlClient).isNotNull();
    }
  }

  @Test
  void createStorageControlClient_withoutCredentials_createsClient() throws Exception {
    try (MockedStatic<StorageControlClient> mocked = mockStatic(StorageControlClient.class)) {
      mocked
          .when(() -> StorageControlClient.create(any(StorageControlSettings.class)))
          .thenReturn(mockControlClient);

      assertThat(gcsClient.createStorageControlClient(Optional.empty()))
          .isSameInstanceAs(mockControlClient);
    }
  }

  @Test
  void lazyGetStorageControlClient_initializesWhenNullAndReusesInstance() throws Exception {
    assertLazyInitializationReusesInstance(
        clientWithMock::lazyGetStorageControlClient,
        () -> clientWithMock.storageControlClient,
        mockControlClient);
  }

  @Test
  void lazyGetStorageControlClient_concurrentCalls_instantiatesExactlyOnce() throws Exception {
    StorageControlClient mockClientInstance = mock(StorageControlClient.class);
    AtomicInteger factoryInvocations = new AtomicInteger();
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry) {
          @Override
          protected StorageControlClient createStorageControlClient(
              Optional<Credentials> credentials) {
            factoryInvocations.incrementAndGet();
            return mockClientInstance;
          }
        };

    assertConcurrentInitializationInstantiatesExactlyOnce(
        client::lazyGetStorageControlClient, factoryInvocations, mockClientInstance);
  }

  @Test
  void lazyGetGrpcStorage_initializesWhenNullAndReusesInstance() throws Exception {
    assertLazyInitializationReusesInstance(
        clientWithMock::lazyGetGrpcStorage, () -> clientWithMock.grpcStorage, mockStorage);
  }

  @Test
  void lazyGetGrpcStorage_bidiReadEnabled_returnsStorageDirectly() {
    GcsClientOptions options =
        GcsClientOptions.builder()
            .setProjectId(TEST_PROJECT)
            .setGcsReadOptions(GcsReadOptions.builder().setBidiReadEnabled(true).build())
            .build();
    GcsClientImpl client =
        new GcsClientImpl(options, executorServiceSupplier, telemetry) {
          @Override
          protected Storage createStorage(Optional<Credentials> credentials) {
            return mockStorage;
          }
        };

    Storage actualStorage = client.lazyGetGrpcStorage();

    assertThat(actualStorage).isSameInstanceAs(mockStorage);
    assertThat(client.grpcStorage).isNull();
  }

  @Test
  void lazyGetGrpcStorage_concurrentCalls_instantiatesExactlyOnce() throws Exception {
    Storage mockStorageInstance = mock(Storage.class);
    AtomicInteger factoryInvocations = new AtomicInteger();
    GcsClientImpl client =
        new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry) {
          @Override
          protected Storage createGrpcStorage(Optional<Credentials> credentials) {
            factoryInvocations.incrementAndGet();
            return mockStorageInstance;
          }
        };

    assertConcurrentInitializationInstantiatesExactlyOnce(
        client::lazyGetGrpcStorage, factoryInvocations, mockStorageInstance);
  }

  @FunctionalInterface
  private interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  private <T> void assertLazyInitializationReusesInstance(
      ThrowingSupplier<T> lazyClientGetter, Supplier<T> clientAccessor, T expectedInstance)
      throws Exception {
    T createdInstance = lazyClientGetter.get();
    T cachedInstance = lazyClientGetter.get();

    assertThat(createdInstance).isSameInstanceAs(expectedInstance);
    assertThat(cachedInstance).isSameInstanceAs(expectedInstance);
    assertThat(clientAccessor.get()).isSameInstanceAs(expectedInstance);
  }

  private <T> void assertConcurrentInitializationInstantiatesExactlyOnce(
      ThrowingSupplier<T> lazyClientGetter, AtomicInteger factoryInvocations, T expectedInstance)
      throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    Callable<T> task =
        () -> {
          ready.countDown();
          start.await();
          return lazyClientGetter.get();
        };

    try {
      Future<T> f1 = executor.submit(task);
      Future<T> f2 = executor.submit(task);
      ready.await(5, TimeUnit.SECONDS);
      start.countDown();

      assertThat(f1.get(5, TimeUnit.SECONDS)).isSameInstanceAs(expectedInstance);
      assertThat(f2.get(5, TimeUnit.SECONDS)).isSameInstanceAs(expectedInstance);
      assertThat(factoryInvocations.get()).isEqualTo(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void getFolderInfo_notFound_returnsNotFoundItemInfo() throws IOException {
    GcsItemId folderItemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_NON_EXISTENT_OBJECT)
            .build();
    NotFoundException notFoundException = mock(NotFoundException.class);
    when(mockControlClient.getFolder(any(GetFolderRequest.class))).thenThrow(notFoundException);

    GcsItemInfo itemInfo = clientWithMock.getFolderInfo(folderItemId);

    assertNotFound(itemInfo, folderItemId);
  }

  @Test
  void getFolderInfo_otherRpcException_throwsIOException() throws IOException {
    when(mockControlClient.getFolder(any(GetFolderRequest.class)))
        .thenThrow(new RuntimeException("RPC error"));

    IOException e =
        assertThrows(IOException.class, () -> clientWithMock.getFolderInfo(TEST_ITEM_ID));

    assertThat(e).hasMessageThat().contains("Failed to get folder info for: " + TEST_ITEM_ID);
  }

  private GcsClientImpl createClientWithMocks(
      Storage mockStorage, StorageControlClient mockControlClient) {
    return new GcsClientImpl(TEST_GCS_CLIENT_OPTIONS, executorServiceSupplier, telemetry) {
      @Override
      protected Storage createStorage(Optional<Credentials> credentials) {
        return mockStorage;
      }

      @Override
      protected Storage createGrpcStorage(Optional<Credentials> credentials) {
        return mockStorage;
      }

      @Override
      protected StorageControlClient createStorageControlClient(Optional<Credentials> credentials) {
        return mockControlClient;
      }
    };
  }

  @Test
  void createGrpcStorage_withCredentials_createsClient() throws Exception {
    try (Storage grpcClient =
        gcsClient.createGrpcStorage(Optional.of(NoCredentials.getInstance()))) {
      assertThat(grpcClient).isNotNull();
    }
  }

  @Test
  void listFirstObjectWithPrefix_hasObjects_returnsSingleItem() throws IOException {
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR).build();
    Blob mockBlob = createMockBlob(TEST_BUCKET, TEST_DIR + "file.txt");
    setupMockStorageWithBlobs(TEST_BUCKET, mockBlob);

    List<GcsItemInfo> result = clientWithMock.listFirstObjectWithPrefix(prefixId);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).getItemId().getObjectName()).hasValue(TEST_DIR + "file.txt");
    assertThat(result.get(0).getSize()).isEqualTo(0L);
  }

  @Test
  void listFirstObjectWithPrefix_empty_returnsEmptyList() throws IOException {
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_DIR).build();
    setupMockStorageWithBlobs(TEST_BUCKET);

    List<GcsItemInfo> result = clientWithMock.listFirstObjectWithPrefix(prefixId);

    assertThat(result).isEmpty();
  }

  @Test
  void listFirstObjectWithPrefix_nullPrefixId_throwsNullPointerException() {
    NullPointerException e =
        assertThrows(NullPointerException.class, () -> gcsClient.listFirstObjectWithPrefix(null));

    assertThat(e).hasMessageThat().contains("prefixId must not be null");
  }

  @Test
  void listFirstObjectWithPrefix_storageThrows404_throwsFileNotFoundException() {
    when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
        .thenThrow(new StorageException(404, "Bucket not found"));

    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> clientWithMock.listFirstObjectWithPrefix(TEST_ITEM_ID));

    assertThat(e).hasMessageThat().contains("Bucket not found: " + TEST_BUCKET);
  }

  @Test
  void listFirstObjectWithPrefix_storageThrows500_throwsIOException() {
    when(mockStorage.list(eq(TEST_BUCKET), any(Storage.BlobListOption[].class)))
        .thenThrow(new StorageException(500, "Internal error"));

    IOException e =
        assertThrows(
            IOException.class, () -> clientWithMock.listFirstObjectWithPrefix(TEST_ITEM_ID));

    assertThat(e)
        .hasMessageThat()
        .contains("Failed to list the first object for prefix: " + TEST_ITEM_ID);
  }

  private static Blob createMockBlob(String bucket, String name) {
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.getBucket()).thenReturn(bucket);
    when(mockBlob.getName()).thenReturn(name);
    return mockBlob;
  }

  private static Bucket createMockBucket(String bucketName) {
    Bucket mockBucket = mock(Bucket.class);
    when(mockBucket.getName()).thenReturn(bucketName);
    when(mockBucket.getLocation()).thenReturn(TEST_LOCATION);
    when(mockBucket.getMetageneration()).thenReturn(2L);
    when(mockBucket.getCreateTimeOffsetDateTime())
        .thenReturn(OffsetDateTime.parse("2026-08-01T10:00:00Z"));
    when(mockBucket.getUpdateTimeOffsetDateTime())
        .thenReturn(OffsetDateTime.parse("2026-08-02T10:00:00Z"));
    return mockBucket;
  }

  private static Folder createMockFolder(String bucketName, String folderName) {
    return Folder.newBuilder()
        .setName("projects/_/buckets/" + bucketName + "/folders/" + folderName)
        .setMetageneration(3L)
        .setCreateTime(Timestamp.newBuilder().setSeconds(1000).setNanos(500).build())
        .setUpdateTime(Timestamp.newBuilder().setSeconds(2000).setNanos(500).build())
        .build();
  }

  @SuppressWarnings("unchecked")
  private void setupMockStorageWithBlobs(String bucket, Blob... blobs) {
    Page<Blob> mockPage = mock(Page.class);
    when(mockPage.getValues()).thenReturn(ImmutableList.copyOf(blobs));
    when(mockStorage.list(eq(bucket), any(Storage.BlobListOption[].class))).thenReturn(mockPage);
  }

  private static void assertNotFound(GcsItemInfo itemInfo, GcsItemId expectedItemId) {
    assertThat(itemInfo.getItemId()).isEqualTo(expectedItemId);
    assertThat(itemInfo.exists()).isFalse();
    assertThat(itemInfo.getSize()).isEqualTo(-1L);
  }
}
