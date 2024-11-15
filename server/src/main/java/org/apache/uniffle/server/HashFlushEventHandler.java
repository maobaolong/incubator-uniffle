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

package org.apache.uniffle.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.function.ConsumerWithException;
import org.apache.uniffle.server.storage.StorageManager;

public class HashFlushEventHandler extends DefaultFlushEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HashFlushEventHandler.class);

  private final int localFileThreadPoolSize;
  private final List<Executor> localFileThreadPoolExecutors;

  public HashFlushEventHandler(
      ShuffleServerConf conf,
      StorageManager storageManager,
      ShuffleServer shuffleServer,
      ConsumerWithException<ShuffleDataFlushEvent> eventConsumer) {
    super(conf, storageManager, shuffleServer, eventConsumer);
    this.localFileThreadPoolSize =
        conf.getInteger(ShuffleServerConf.SERVER_FLUSH_LOCALFILE_THREAD_POOL_SIZE);
    this.localFileThreadPoolExecutors = new ArrayList<>();
    for (int i = 0; i < localFileThreadPoolSize; i++) {
      // Note: Do not change the supplier num.
      //    each thread pool has only one thread, and the thread does not add a mutex lock when
      // processing events.
      localFileThreadPoolExecutors.add(
          createFlushEventExecutor(() -> 1, "LocalFileFlushEventThreadPool_" + i));
    }
  }

  protected void initLocalFileFlushEventExecutor() {}

  protected Executor getLocalFileThreadPoolExecutor(ShuffleDataFlushEvent event) {
    String key =
        event.getAppId()
            + event.getShuffleId()
            + event.getStartPartition()
            + event.getEndPartition();
    int index = (key.hashCode() & Integer.MAX_VALUE) % localFileThreadPoolSize;
    ShuffleServerMetrics.gaugeLocalfileFlushThreadPoolQueueSize.inc();
    return localFileThreadPoolExecutors.get(index);
  }

  @Override
  public boolean isWithLock() {
    return false;
  }
}
