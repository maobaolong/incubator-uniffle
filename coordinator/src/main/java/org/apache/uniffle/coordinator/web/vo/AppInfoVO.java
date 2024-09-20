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

package org.apache.uniffle.coordinator.web.vo;

import java.util.Map;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import org.apache.uniffle.coordinator.ShuffleInfo;

@Data
@Builder
@AllArgsConstructor
@Getter
@Setter
public class AppInfoVO implements Comparable<AppInfoVO> {
  private String userName;
  private String appId;
  private long updateTime;
  private long registrationTime;
  private String exitCode;
  private long finishTime;
  private String version;
  private String gitCommitId;

  private long partitionNum;
  private long memorySize;
  private long localFileNum;
  private long localTotalSize;
  private long hadoopFileNum;
  private long hadoopTotalSize;
  private long totalSize;

  private String url;

  private Map<String, String> displayAppConf;

  private Map<String, String> appConf;

  private Map<Integer, ShuffleInfo> shuffleInfo;

  private String maxSizePartitionInfo;

  public AppInfoVO() {}

  @Override
  public int compareTo(AppInfoVO appInfoVO) {
    return Long.compare(registrationTime, appInfoVO.getRegistrationTime());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AppInfoVO)) {
      return false;
    }
    AppInfoVO appInfoVO = (AppInfoVO) o;
    return updateTime == appInfoVO.updateTime
        && registrationTime == appInfoVO.registrationTime
        && userName.equals(appInfoVO.userName)
        && appId.equals(appInfoVO.appId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userName, appId, updateTime, registrationTime);
  }
}
