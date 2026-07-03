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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GcsItemInfoTest {

  @Test
  public void testIsInferredDirectory() {
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(GcsItemId.builder().setBucketName("bucket").setObjectName("dir/").build())
            .setItemType(GcsItemInfo.ItemType.INFERRED_DIRECTORY)
            .build();

    assertThat(itemInfo.isInferredDirectory()).isTrue();
    assertThat(itemInfo.isNativeHnsFolder()).isFalse();
  }

  @Test
  public void testIsNativeHnsFolder() {
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(GcsItemId.builder().setBucketName("bucket").setObjectName("folder/").build())
            .setItemType(GcsItemInfo.ItemType.NATIVE_FOLDER)
            .build();

    assertThat(itemInfo.isInferredDirectory()).isFalse();
    assertThat(itemInfo.isNativeHnsFolder()).isTrue();
  }

  @Test
  public void testIsObject() {
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(GcsItemId.builder().setBucketName("bucket").setObjectName("obj").build())
            .setItemType(GcsItemInfo.ItemType.OBJECT)
            .build();

    assertThat(itemInfo.isInferredDirectory()).isFalse();
    assertThat(itemInfo.isNativeHnsFolder()).isFalse();
  }
}
