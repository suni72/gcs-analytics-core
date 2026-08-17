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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchExecutor provides a means to manually batch requests using a thread pool. Execution is
 * performed by the underlying requestsExecutor ExecutorService.
 */
final class BatchExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(BatchExecutor.class);

  /** Keep-alive time for core threads. */
  private static final long KEEP_ALIVE_SECONDS = 10L;

  /** Multiplier bounding the task queue size relative to thread count. */
  private static final int QUEUE_CAPACITY_PER_THREAD = 20;

  /**
   * Grace period for thread pool shutdown. Since awaitRequestsCompletion() already waits for all
   * tasks to complete, threads should be idle and terminate instantaneously.
   */
  private static final long SHUTDOWN_GRACE_PERIOD_SECONDS = 1L;

  private final ExecutorService requestsExecutor;
  private final Queue<Future<Void>> responseFutures = new ConcurrentLinkedQueue<>();

  BatchExecutor(int numThreads) {
    this.requestsExecutor =
        numThreads == 0 ? newDirectExecutorService() : newRequestExecutor(numThreads);
  }

  private static ExecutorService newRequestExecutor(int numThreads) {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            /* corePoolSize= */ numThreads,
            /* maximumPoolSize= */ numThreads,
            KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(numThreads * QUEUE_CAPACITY_PER_THREAD),
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-batch-executor-pool-%d")
                .setDaemon(true)
                .build());
    executor.allowCoreThreadTimeOut(true);
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    return executor;
  }

  /** Adds a task to the execution queue. */
  public <T> void queue(Callable<T> task, FutureCallback<T> callback) {
    checkState(
        !requestsExecutor.isShutdown() && !requestsExecutor.isTerminated(),
        "requestExecutor should not be terminated to queue request");

    responseFutures.add(
        requestsExecutor.submit(
            () -> {
              execute(task, callback);
              return null;
            }));
  }

  private static <T> void execute(Callable<T> task, FutureCallback<T> callback) throws Exception {
    try {
      T result = task.call();
      if (callback != null) {
        callback.onSuccess(result);
      }
    } catch (Throwable throwable) {
      if (callback != null) {
        callback.onFailure(throwable);
      } else {
        throw throwable;
      }
    }
  }

  /**
   * Checks if the underlying executor has any active or queued tasks. This is used to detect
   * stalling issues in jobs.
   */
  public boolean isIdle() {
    if (requestsExecutor instanceof ThreadPoolExecutor) {
      ThreadPoolExecutor executor = (ThreadPoolExecutor) requestsExecutor;
      return executor.getActiveCount() == 0 && executor.getQueue().isEmpty();
    }
    // The executor is a DirectExecutorService, it is always idle because there are no child
    // threads.
    return true;
  }

  /** Awaits completion of all queued tasks and shuts down the executor. */
  public void shutdown() throws IOException {
    try {
      awaitRequestsCompletion();
      checkState(responseFutures.isEmpty(), "responseFutures should be empty after await");
    } finally {
      requestsExecutor.shutdown();
      try {
        if (!requestsExecutor.awaitTermination(SHUTDOWN_GRACE_PERIOD_SECONDS, TimeUnit.SECONDS)) {
          LOG.warn("Forcibly shutting down batch executor thread pool.");
          requestsExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.debug(
            "Interrupted awaiting termination: forcibly shutting down batch executor thread pool.",
            e);
        requestsExecutor.shutdownNow();
      }
    }
  }

  /** Awaits until all sent requests are completed. Should be serialized */
  private void awaitRequestsCompletion() throws IOException {
    while (!responseFutures.isEmpty()) {
      getFromFuture(responseFutures.remove());
    }
  }

  /** Retrieves the result of a Future, converting exceptions to IOException. */
  private static void getFromFuture(Future<Void> future) throws IOException {
    try {
      future.get();
    } catch (ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      } else if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new IOException("Batch task failed", e);
    }
  }
}
