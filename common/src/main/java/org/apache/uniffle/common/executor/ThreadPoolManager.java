/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.common.executor;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.metrics.CommonMetrics;

/** The threadPool manager which represents a manager to handle all thread pool executors. */
public class ThreadPoolManager {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadPoolManager.class);

  private static final Map<Object, ReconfigurableThreadPoolExecutor> THREAD_POOL_MAP =
      new ConcurrentHashMap<>();

  /**
   * Add a thread pool.
   *
   * @param name the name of the thread pool
   * @param corePoolSizeSupplier the core pool size supplier
   * @param maximumPoolSizeSupplier the maximum pool size supplier
   * @param keepAliveTimeSupplier the keep alive time supplier
   * @param unit the unit
   * @param workQueue the work queue
   * @param threadFactory the thread factory
   * @return the registered thread pool
   */
  public static ThreadPoolExecutor newThreadPool(
      String name,
      Supplier<Integer> corePoolSizeSupplier,
      Supplier<Integer> maximumPoolSizeSupplier,
      Supplier<Long> keepAliveTimeSupplier,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory) {
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            corePoolSizeSupplier.get(),
            maximumPoolSizeSupplier.get(),
            keepAliveTimeSupplier.get(),
            unit,
            workQueue,
            threadFactory);
    registerThreadPool(
        name,
        corePoolSizeSupplier,
        maximumPoolSizeSupplier,
        keepAliveTimeSupplier,
        threadPoolExecutor);
    return threadPoolExecutor;
  }

  /**
   * Add a thread pool.
   *
   * @param name the name of the thread pool
   * @param corePoolSizeSupplier the core pool size supplier
   * @param maximumPoolSizeSupplier the maximum pool size supplier
   * @param keepAliveTimeSupplier the keep alive time supplier
   * @param unit the unit
   * @param workQueue the work queue
   * @param threadFactory the thread factory
   * @param handler the handler to use when execution is blocked because the thread bounds and queue
   *     capacities are reached
   * @return the registered thread pool
   */
  public static ThreadPoolExecutor newThreadPool(
      String name,
      Supplier<Integer> corePoolSizeSupplier,
      Supplier<Integer> maximumPoolSizeSupplier,
      Supplier<Long> keepAliveTimeSupplier,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            corePoolSizeSupplier.get(),
            maximumPoolSizeSupplier.get(),
            keepAliveTimeSupplier.get(),
            unit,
            workQueue,
            threadFactory,
            handler);
    registerThreadPool(
        name,
        corePoolSizeSupplier,
        maximumPoolSizeSupplier,
        keepAliveTimeSupplier,
        threadPoolExecutor);
    return threadPoolExecutor;
  }

  /**
   * Register a thread pool to THREAD_POOL_MAP.
   *
   * @param name the name of the thread pool
   * @param corePoolSizeSupplier the core pool size supplier
   * @param maximumPoolSizeSupplier the maximum pool size supplier
   * @param keepAliveTimeSupplier the keep alive time supplier
   * @param threadPoolExecutor the thread pool which will be registered
   */
  public static void registerThreadPool(
      String name,
      Supplier<Integer> corePoolSizeSupplier,
      Supplier<Integer> maximumPoolSizeSupplier,
      Supplier<Long> keepAliveTimeSupplier,
      ThreadPoolExecutor threadPoolExecutor) {
    THREAD_POOL_MAP.put(
        threadPoolExecutor,
        new ReconfigurableThreadPoolExecutor(
            name,
            threadPoolExecutor,
            corePoolSizeSupplier,
            maximumPoolSizeSupplier,
            keepAliveTimeSupplier));
    LOG.info(
        "{} thread pool, core size:{}, max size:{}, keep alive time:{}",
        name,
        corePoolSizeSupplier.get(),
        maximumPoolSizeSupplier.get(),
        keepAliveTimeSupplier.get());
  }

  /**
   * Unregister the thread pool executor related to the given key.
   *
   * @param key the key of thread pool executor to unregister
   */
  public static void unregister(Object key) {
    ReconfigurableThreadPoolExecutor reconfigurableThreadPoolExecutor = THREAD_POOL_MAP.remove(key);
    if (reconfigurableThreadPoolExecutor != null) {
      reconfigurableThreadPoolExecutor.close();
    }
  }

  public static boolean exists(Object key) {
    return THREAD_POOL_MAP.containsKey(key);
  }

  private static class ReconfigurableThreadPoolExecutor implements Closeable {
    private final String mName;
    private final ThreadPoolExecutor mThreadPoolExecutor;
    private final Supplier<Integer> mCorePoolSizeSupplier;
    private final Supplier<Integer> mMaximumPoolSizeSupplier;
    private final Supplier<Long> mKeepAliveTimeSupplier;

    ReconfigurableThreadPoolExecutor(
        String name,
        ThreadPoolExecutor threadPoolExecutor,
        Supplier<Integer> corePoolSizeSupplier,
        Supplier<Integer> maximumPoolSizeSupplier,
        Supplier<Long> keepAliveTimeSupplier) {
      // TODO(baoloongmao): implements reconfigurable thread pool
      mName = name;
      mThreadPoolExecutor = threadPoolExecutor;
      mCorePoolSizeSupplier = corePoolSizeSupplier;
      mMaximumPoolSizeSupplier = maximumPoolSizeSupplier;
      mKeepAliveTimeSupplier = keepAliveTimeSupplier;

      MeasurableRejectedExecutionHandler measurableRejectedExecutionHandler =
          new MeasurableRejectedExecutionHandler(threadPoolExecutor.getRejectedExecutionHandler());
      threadPoolExecutor.setRejectedExecutionHandler(measurableRejectedExecutionHandler);
      CommonMetrics.addLabeledCacheGauge(
          name + "_ThreadActiveCount", threadPoolExecutor::getActiveCount, 5_000L);
      CommonMetrics.addLabeledCacheGauge(
          name + "_ThreadCurrentCount", threadPoolExecutor::getPoolSize, 5_000L);
      CommonMetrics.addLabeledCacheGauge(
          name + "_ThreadMaxCount", threadPoolExecutor::getMaximumPoolSize, 5_000L);
      CommonMetrics.addLabeledCacheGauge(
          name + "_ThreadMinCount", threadPoolExecutor::getCorePoolSize, 5_000L);
      CommonMetrics.addLabeledCacheGauge(
          name + "_CompleteTaskCount", threadPoolExecutor::getCompletedTaskCount, 5_000L);
      CommonMetrics.addLabeledCacheGauge(
          name + "_ThreadQueueWaitingTaskCount", threadPoolExecutor.getQueue()::size, 5_000L);
      CommonMetrics.addLabeledCacheGauge(
          name + "_RejectCount", measurableRejectedExecutionHandler::getCount, 5_000L);
    }

    @Override
    public void close() {
      // TODO(baoloongmao): remove these metrics from map
    }
  }
}
