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

import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.common.function.ConsumerWithException;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.storage.StorageManager;

public class FlushEventHandlerFactory {
  public static FlushEventHandler createFlushEventHandler(
      ShuffleServerConf conf,
      StorageManager storageManager,
      ShuffleServer shuffleServer,
      ConsumerWithException<ShuffleDataFlushEvent> eventConsumer) {
    String className = conf.get(ShuffleServerConf.SERVER_FLUSH_EVENT_HANDLE_STRATEGY_CLASS);
    return createFlushEventHandler(
        className,
        ShuffleServerConf.SERVER_FLUSH_EVENT_HANDLE_STRATEGY_CLASS.key(),
        conf,
        storageManager,
        shuffleServer,
        eventConsumer);
  }

  public static FlushEventHandler createFlushEventHandler(
      String className,
      String configKey,
      ShuffleServerConf conf,
      StorageManager storageManager,
      ShuffleServer shuffleServer,
      ConsumerWithException<ShuffleDataFlushEvent> eventConsumer) {
    if (StringUtils.isEmpty(className)) {
      throw new IllegalStateException(
          "Configuration error: " + configKey + " should not set to empty");
    }

    try {
      return (FlushEventHandler)
          RssUtils.getConstructor(
                  className,
                  ShuffleServerConf.class,
                  StorageManager.class,
                  ShuffleServer.class,
                  ConsumerWithException.class)
              .newInstance(conf, storageManager, shuffleServer, eventConsumer);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Configuration error: " + configKey + " is failed to create instance of " + className, e);
    }
  }
}
