package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FlatNamespaceStrategyImplTest {

  private static final String TEST_BUCKET = "test-bucket";

  @Mock private GcsClient mockClient;

  private FlatNamespaceStrategyImpl strategy;

  @BeforeEach
  void setUp() {
    strategy =
        new FlatNamespaceStrategyImpl(mockClient, () -> MoreExecutors.newDirectExecutorService());
  }

  @Test
  void getFileInfo_withDirectMatch_returnsFileInfo() throws Exception {
    GcsItemId id =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/file.txt").build();
    GcsItemInfo mockInfo =
        GcsItemInfo.builder().setItemId(id).setSize(100L).setContentGeneration(1L).build();

    when(mockClient.getGcsItemInfo(id)).thenReturn(mockInfo);
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/file.txt/").build();
    when(mockClient.listObjectInfo(prefixId, 1)).thenReturn(Collections.emptyList());

    GcsItemInfo result = strategy.getFileInfo(id, PathType.UNKNOWN);

    assertThat(result).isEqualTo(mockInfo);
    assertThat(result.getItemId().getObjectName().get()).isEqualTo("dir/file.txt");
  }

  @Test
  void getFileInfo_withPrefixMatch_returnsDirectory() throws Exception {
    GcsItemId id =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();

    when(mockClient.getGcsItemInfo(id)).thenThrow(new IOException("Not found"));
    GcsItemId childId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/file.txt").build();
    GcsItemInfo childInfo =
        GcsItemInfo.builder().setItemId(childId).setSize(10L).setContentGeneration(1L).build();
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();
    when(mockClient.listObjectInfo(prefixId, 1)).thenReturn(List.of(childInfo));

    GcsItemInfo result = strategy.getFileInfo(id, PathType.UNKNOWN);

    assertThat(result.getItemId().getObjectName().get()).isEqualTo("dir/subdir/");
  }

  @Test
  void getFileInfo_withExplicitDirectoryPathType_usesOnlyListObjectInfo() throws Exception {
    GcsItemId id =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();

    GcsItemId childId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/file.txt").build();
    GcsItemInfo childInfo =
        GcsItemInfo.builder().setItemId(childId).setSize(10L).setContentGeneration(1L).build();
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();
    when(mockClient.listObjectInfo(prefixId, 1)).thenReturn(List.of(childInfo));

    GcsItemInfo result = strategy.getFileInfo(id, PathType.DIRECTORY);

    assertThat(result.getItemId().getObjectName().get()).isEqualTo("dir/subdir/");
  }

  @Test
  void getFileInfo_notFound_throwsIOException() throws Exception {
    GcsItemId id =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();

    when(mockClient.getGcsItemInfo(id)).thenThrow(new IOException("Not found"));
    GcsItemId prefixId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();
    when(mockClient.listObjectInfo(prefixId, 1)).thenReturn(Collections.emptyList());

    assertThrows(IOException.class, () -> strategy.getFileInfo(id, PathType.UNKNOWN));
  }
}
