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

package org.apache.uniffle.common.audit;

import java.io.Closeable;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.prometheus.client.Counter;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import org.apache.uniffle.common.rpc.StatusCode;

import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGEEightySecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGEFiftySecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGEFortySecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGEHundredSecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGENinetySecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGEOneSecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGESeventySecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGESixtySecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGETenSecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGEThirtySecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeGETwentySecond;
import static org.apache.uniffle.common.metrics.RPCMetrics.counterRpcExecutionTimeLTOneSecond;

/** Context for rpc audit logging. */
public abstract class RpcAuditContext implements Closeable {
  private final Logger log;
  private String command;
  private String statusCode;
  private String args;
  private String returnValue;
  private String from;
  private long creationTimeNs;
  protected long executionTimeNs;
  protected String executionTimeLevel;
  protected static final RangeMap<Long, Pair<String, Counter.Child>> executionTimeNsLevelMap =
      TreeRangeMap.create();

  static {
    long[] thresholds = {
      0L,
      1000000000L,
      10000000000L,
      20000000000L,
      30000000000L,
      40000000000L,
      50000000000L,
      60000000000L,
      70000000000L,
      80000000000L,
      90000000000L,
      100000000000L
    };

    String[] labels = {
      "<1s", ">=1s", ">=10s", ">=20s", ">=30s", ">=40s", ">=50s", ">=60s", ">=70s", ">=80s",
      ">=90s", ">=100s"
    };

    Counter.Child[] counters = {
      counterRpcExecutionTimeLTOneSecond,
      counterRpcExecutionTimeGEOneSecond,
      counterRpcExecutionTimeGETenSecond,
      counterRpcExecutionTimeGETwentySecond,
      counterRpcExecutionTimeGEThirtySecond,
      counterRpcExecutionTimeGEFortySecond,
      counterRpcExecutionTimeGEFiftySecond,
      counterRpcExecutionTimeGESixtySecond,
      counterRpcExecutionTimeGESeventySecond,
      counterRpcExecutionTimeGEEightySecond,
      counterRpcExecutionTimeGENinetySecond,
      counterRpcExecutionTimeGEHundredSecond
    };

    for (int i = 0; i < thresholds.length - 1; i++) {
      executionTimeNsLevelMap.put(
          Range.closedOpen(thresholds[i], thresholds[i + 1]), Pair.of(labels[i], counters[i]));
    }
    executionTimeNsLevelMap.put(
        Range.closed(thresholds[thresholds.length - 1], Long.MAX_VALUE),
        Pair.of(labels[labels.length - 1], counters[labels.length - 1]));
  }

  public RpcAuditContext(Logger log) {
    this.log = log;
  }

  protected abstract String content();

  /**
   * Sets mCommand field.
   *
   * @param command the command associated with shuffle server rpc
   * @return this {@link RpcAuditContext} instance
   */
  public RpcAuditContext withCommand(String command) {
    this.command = command;
    return this;
  }

  /**
   * Sets creationTimeNs field.
   *
   * @param creationTimeNs the System.nanoTime() when this operation create, it only can be used to
   *     compute operation mExecutionTime
   * @return this {@link RpcAuditContext} instance
   */
  public RpcAuditContext withCreationTimeNs(long creationTimeNs) {
    this.creationTimeNs = creationTimeNs;
    return this;
  }

  /**
   * Sets status code field.
   *
   * @param statusCode the status code
   * @return this {@link RpcAuditContext} instance
   */
  public RpcAuditContext withStatusCode(StatusCode statusCode) {
    if (statusCode == null) {
      this.statusCode = "UNKNOWN";
    } else {
      this.statusCode = statusCode.name();
    }
    return this;
  }

  /**
   * Sets status code field.
   *
   * @param statusCode the status code
   * @return this {@link RpcAuditContext} instance
   */
  public RpcAuditContext withStatusCode(org.apache.uniffle.proto.RssProtos.StatusCode statusCode) {
    if (statusCode == null) {
      this.statusCode = "UNKNOWN";
    } else {
      this.statusCode = statusCode.name();
    }
    return this;
  }

  /**
   * Sets status code field.
   *
   * @param statusCode the status code
   * @return this {@link RpcAuditContext} instance
   */
  public RpcAuditContext withStatusCode(String statusCode) {
    this.statusCode = statusCode;
    return this;
  }

  public RpcAuditContext withArgs(String args) {
    this.args = args;
    return this;
  }

  public RpcAuditContext withReturnValue(String returnValue) {
    this.returnValue = returnValue;
    return this;
  }

  public RpcAuditContext withFrom(String from) {
    this.from = from;
    return this;
  }

  @Override
  public void close() {
    executionTimeNs = System.nanoTime() - creationTimeNs;
    Pair<String, Counter.Child> pair = executionTimeNsLevelMap.get(executionTimeNs);
    if (pair != null) {
      executionTimeLevel = pair.getLeft();
      Counter.Child counter = pair.getRight();
      if (counter != null) {
        counter.inc();
      }
    }

    if (log == null) {
      return;
    }
    log.info(toString());
  }

  @Override
  public String toString() {
    String line =
        String.format(
            "cmd=%s\tstatusCode=%s\tfrom=%s\texecutionTimeUs=%d(%s)\t%s",
            command, statusCode, from, executionTimeNs / 1000, executionTimeLevel, content());
    if (args != null) {
      line += String.format("\targs{%s}", args);
    }
    if (returnValue != null) {
      line += String.format("\treturn{%s}", returnValue);
    }
    return line;
  }
}
