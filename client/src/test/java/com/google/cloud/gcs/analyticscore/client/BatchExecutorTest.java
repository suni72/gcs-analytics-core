/*
 * Copyright 2026 Google LLC
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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BatchExecutorTest {

  private BatchExecutor batchExecutor;

  @BeforeEach
  public void setUp() {
    batchExecutor = new BatchExecutor(10);
  }

  @Test
  public void queue_withDirectExecutor_executesSynchronouslyAndIsAlwaysIdle() throws Exception {
    BatchExecutor executor = new BatchExecutor(0);
    List<Boolean> results = new ArrayList<>();

    executor.queue(() -> true, recordCallback(results, null));
    executor.shutdown();

    assertThat(results).containsExactly(true);
    assertThat(executor.isIdle()).isTrue();
  }

  @Test
  public void queue_executesMultipleTasksConcurrently() throws Exception {
    List<Integer> results = new CopyOnWriteArrayList<>();

    for (int i = 0; i < 10; i++) {
      final int value = i;
      batchExecutor.queue(() -> value, recordCallback(results, null));
    }
    batchExecutor.shutdown();

    assertThat(results).hasSize(10);
  }

  @Test
  public void queue_taskThrowsException_callsOnFailureWhenCallbackProvided() throws Exception {
    SettableFuture<Throwable> failure = SettableFuture.create();

    batchExecutor.queue(
        () -> {
          throw new IOException("simulated failure");
        },
        recordCallback(null, failure));
    batchExecutor.shutdown();

    assertThat(failure.get()).isInstanceOf(IOException.class);
    assertThat(failure.get()).hasMessageThat().isEqualTo("simulated failure");
  }

  @Test
  public void queue_taskThrowsException_propagateOnShutdownWhenCallbackIsNull() {
    batchExecutor.queue(
        () -> {
          throw new IOException("simulated failure");
        },
        null);

    IOException exception = assertThrows(IOException.class, batchExecutor::shutdown);

    assertThat(exception).hasMessageThat().isEqualTo("simulated failure");
  }

  @Test
  public void queue_taskThrowsRuntimeException_wrapsInIOExceptionOnShutdown() {
    batchExecutor.queue(
        () -> {
          throw new RuntimeException("runtime error");
        },
        null);

    IOException exception = assertThrows(IOException.class, batchExecutor::shutdown);

    assertThat(exception).hasMessageThat().isEqualTo("Batch task failed");
    assertThat(exception.getCause()).isInstanceOf(ExecutionException.class);
    assertThat(exception.getCause().getCause()).hasMessageThat().isEqualTo("runtime error");
  }

  @Test
  public void queue_afterShutdown_throwsIllegalStateException() throws Exception {
    batchExecutor.shutdown();

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> batchExecutor.queue(() -> "task", null));

    assertThat(exception)
        .hasMessageThat()
        .startsWith("requestExecutor should not be terminated to queue request");
  }

  @Test
  public void isIdle_forThreadPoolExecutor_returnsCorrectState() throws Exception {
    SettableFuture<Void> taskStarted = SettableFuture.create();
    SettableFuture<Void> taskShouldFinish = SettableFuture.create();

    assertThat(batchExecutor.isIdle()).isTrue();

    batchExecutor.queue(
        () -> {
          taskStarted.set(null);
          taskShouldFinish.get(1, TimeUnit.SECONDS);
          return null;
        },
        null);
    taskStarted.get(1, TimeUnit.SECONDS);

    assertThat(batchExecutor.isIdle()).isFalse();

    taskShouldFinish.set(null);
    batchExecutor.shutdown();

    assertThat(batchExecutor.isIdle()).isTrue();
  }

  private static <T> FutureCallback<T> recordCallback(
      Collection<T> successes, SettableFuture<Throwable> failure) {
    return new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        if (successes != null) {
          successes.add(result);
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        if (failure != null) {
          failure.set(throwable);
        }
      }
    };
  }
}
