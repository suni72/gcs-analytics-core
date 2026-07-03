package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HierarchicalNamespaceStrategyImplTest {

  private static final String TEST_BUCKET = "test-bucket";

  @Mock private GcsClient mockClient;

  private HierarchicalNamespaceStrategyImpl strategy;

  @BeforeEach
  void setUp() {
    strategy = new HierarchicalNamespaceStrategyImpl(mockClient);
  }

  @Test
  void getFileInfo_withNativeFolder_returnsDirectory() throws Exception {
    GcsItemId id =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();
    GcsItemId dirId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();
    GcsItemInfo folderInfo =
        GcsItemInfo.builder().setItemId(dirId).setSize(0L).setContentGeneration(1L).build();

    when(mockClient.getGcsItemInfo(id)).thenThrow(new IOException("Not found"));
    when(mockClient.getFolderInfo(dirId)).thenReturn(folderInfo);

    GcsItemInfo result = strategy.getFileInfo(id, PathType.UNKNOWN);

    assertThat(result.getItemId().getObjectName().get()).isEqualTo("dir/subdir/");
    assertThat(result).isEqualTo(folderInfo);
  }

  @Test
  void getFileInfo_withLegacyObjectFallback_returnsFileInfo() throws Exception {
    GcsItemId id =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();
    GcsItemId dirId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();

    GcsItemInfo objectInfo =
        GcsItemInfo.builder().setItemId(id).setSize(100L).setContentGeneration(1L).build();
    when(mockClient.getGcsItemInfo(id)).thenReturn(objectInfo);

    GcsItemInfo result = strategy.getFileInfo(id, PathType.UNKNOWN);

    assertThat(result.getItemId().getObjectName().get()).isEqualTo("dir/subdir");
    assertThat(result).isEqualTo(objectInfo);
  }

  @Test
  void getFileInfo_notFound_throwsIOException() throws Exception {
    GcsItemId id =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();
    GcsItemId dirId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();

    when(mockClient.getFolderInfo(dirId)).thenThrow(new IOException("Not a folder"));
    when(mockClient.getGcsItemInfo(id)).thenThrow(new IOException("Not found"));

    assertThrows(IOException.class, () -> strategy.getFileInfo(id, PathType.UNKNOWN));
  }
}
