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

package org.apache.uniffle.coordinator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.ExitUtils.ExitException;
import org.apache.uniffle.coordinator.web.vo.AppInfoVO;

import static org.apache.uniffle.coordinator.AppInfo.createAppInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CoordinatorServerTest {

  @Test
  public void test() throws Exception {
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setInteger("rss.rpc.server.port", 9537);
    coordinatorConf.setInteger("rss.jetty.http.port", 9528);
    coordinatorConf.setInteger("rss.rpc.executor.size", 10);

    CoordinatorServer cs1 = new CoordinatorServer(coordinatorConf);
    CoordinatorServer cs2 = new CoordinatorServer(coordinatorConf);
    cs1.start();

    ExitUtils.disableSystemExit();
    String expectMessage = "Fail to start jetty http server";
    final int expectStatus = 1;
    try {
      cs2.start();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith(expectMessage));
      assertEquals(expectStatus, ((ExitException) e).getStatus());
    } finally {
      // Always call stopServer after new CoordinatorServer to shut down ExecutorService
      cs2.stopServer();
    }

    coordinatorConf.setInteger("rss.jetty.http.port", 9529);
    cs2 = new CoordinatorServer(coordinatorConf);
    expectMessage = "Fail to start grpc server";
    try {
      cs2.start();
    } catch (Exception e) {
      assertEquals(expectMessage, e.getMessage());
      assertEquals(expectStatus, ((ExitException) e).getStatus());
    } finally {
      // Always call stopServer after new CoordinatorServer to shut down ExecutorService
      cs2.stopServer();
      cs1.stopServer();
    }

    final Thread t =
        new Thread(
            null,
            () -> {
              throw new AssertionError("TestUncaughtException");
            },
            "testThread");
    t.start();
    t.join();
  }

  @Test
  public void testAppInfoVOUrl() throws Exception {
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setString("rss.appid.reg.pattern", "(application_\\d+_\\d+)");
    coordinatorConf.setString("rss.appid.url.template", "http://localhost/{appId}/test");
    CoordinatorServer cs1 = new CoordinatorServer(coordinatorConf);
    String user = "user01";
    AppInfo appInfo = createAppInfo("application_1703049085550_19283617_1724740334479", 0);
    AppInfoVO appInfoVO = cs1.getAppInfoV0(user, appInfo);
    assertEquals(appInfoVO.getUrl(), "http://localhost/application_1703049085550_19283617/test");
  }

  @Test
  public void testAppHistoryManager() throws Exception {
    String pathStr = "file://./test_app_history.txt";
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setString(CoordinatorConf.COORDINATOR_APP_HISTORY_PATH, pathStr);
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_APP_HISTORY_FLUSH_INTERVAL_MS, 10);
    coordinatorConf.setInteger(CoordinatorConf.COORDINATOR_APP_HISTORY_FILE_ROTATE_SIZE, 500);
    // Make sure the history file not exist.
    Path path = new Path(CoordinatorAppHistoryManager.convertToHadoopPath(pathStr));
    FileSystem fs =
        HadoopFilesystemProvider.getFilesystem(
            "rss_coordinator_app_history", path, coordinatorConf.getHadoopConf());
    {
      // clean old files
      FileStatus[] allFiles = fs.listStatus(path.getParent());

      // Filter files that match our naming pattern
      List<FileStatus> relevantFiles =
          Arrays.stream(allFiles)
              .filter(file -> file.getPath().getName().startsWith(path.getName()))
              .collect(Collectors.toList());
      for (FileStatus file : relevantFiles) {
        fs.delete(file.getPath(), true);
      }
    }
    try {
      {
        CoordinatorAppHistoryManager appHistoryManager =
            new CoordinatorAppHistoryManager(coordinatorConf);
        List<AppInfoVO> appInfos = appHistoryManager.getAppInfos(10);
        assertEquals(0, appInfos.size());

        Map<Integer, ShuffleInfo> shuffleInfo = new HashMap<>();
        shuffleInfo.put(0, new ShuffleInfo(0, 1728378906904L, 10));

        // add app info
        AppInfoVO appInfoVO =
            new AppInfoVO(
                "user",
                "application_01",
                0,
                0,
                StatusCode.SUCCESS.toString(),
                0,
                "1.0",
                "123456",
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                "http://localhost/application_01/test",
                null,
                null,
                shuffleInfo,
                "");
        appHistoryManager.addAppInfo(appInfoVO);
        Thread.sleep(100); // wait for flush to storage
        appInfos = appHistoryManager.getAppInfos(10);
        assertEquals(appInfos.size(), 1);
      }
      {
        // Load history app from history file
        CoordinatorAppHistoryManager appHistoryManager =
            new CoordinatorAppHistoryManager(coordinatorConf);
        List<AppInfoVO> appInfos = appHistoryManager.getAppInfos(10);
        assertEquals(1, appInfos.size());

        Map<Integer, ShuffleInfo> shuffleInfo = new HashMap<>();
        shuffleInfo.put(1, new ShuffleInfo(1, 1728378906904L, 10));
        // Trigger rotate
        AppInfoVO appInfoVO =
            new AppInfoVO(
                "user",
                "application_02",
                0,
                0,
                StatusCode.SUCCESS.toString(),
                0,
                "1.0",
                "123456",
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                "http://localhost/application_02/test",
                null,
                null,
                null,
                "");
        appHistoryManager.addAppInfo(appInfoVO);
        Thread.sleep(100); // wait for flush to storage
        appInfos = appHistoryManager.getAppInfos(10);
        assertEquals(appInfos.size(), 2);
      }
      {
        // Check the history path, history file has been rotated
        FileStatus[] allFiles = fs.listStatus(path.getParent());
        // Filter files that match our naming pattern
        List<FileStatus> relevantFiles =
            Arrays.stream(allFiles)
                .filter(file -> file.getPath().getName().startsWith(path.getName()))
                .collect(Collectors.toList());
        assertEquals(2, relevantFiles.size());

        // Get the history app num from all history file and rotated file.
        CoordinatorAppHistoryManager appHistoryManager =
            new CoordinatorAppHistoryManager(coordinatorConf);
        List<AppInfoVO> appInfos = appHistoryManager.getAppInfos(10);
        assertEquals(2, appInfos.size());
      }
    } finally {
      // clean up
      FileStatus[] allFiles = fs.listStatus(path.getParent());

      // Filter files that match our naming pattern
      List<FileStatus> relevantFiles =
          Arrays.stream(allFiles)
              .filter(file -> file.getPath().getName().startsWith(path.getName()))
              .collect(Collectors.toList());
      for (FileStatus file : relevantFiles) {
        fs.delete(file.getPath(), true);
      }
    }
  }
}
