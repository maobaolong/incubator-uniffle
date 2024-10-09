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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.coordinator.web.vo.AppInfoVO;
import org.apache.uniffle.storage.handler.impl.HadoopFileReader;
import org.apache.uniffle.storage.handler.impl.HadoopFileWriter;

public class CoordinatorAppHistoryManager {
  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorAppHistoryManager.class);
  private final BlockingQueue<AppInfoVO> appInfoQueue;
  private volatile boolean isRunning;
  private final Map<String, AppInfoVO> cachedAppInfoMap;
  private final int cachedAppInfoSize;
  private final int dashboardAppInfoMaxSize;
  private final CoordinatorConf conf;

  private Path path = null;
  private FileSystem fs = null;
  private HadoopFileWriter hadoopFileWriter = null;
  private int batchSize = 0;
  private long flushIntervalMs = 0;

  private final long rotateSize;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public CoordinatorAppHistoryManager(CoordinatorConf conf) throws Exception {
    this.conf = conf;
    this.isRunning = true;
    this.rotateSize =
        this.conf.getInteger(CoordinatorConf.COORDINATOR_APP_HISTORY_FILE_ROTATE_SIZE);
    this.appInfoQueue =
        new LinkedBlockingQueue<>(
            this.conf.getInteger(CoordinatorConf.COORDINATOR_APP_HISTORY_CACHE_MAX_SIZE));
    this.cachedAppInfoSize =
        this.conf.getInteger(CoordinatorConf.COORDINATOR_APP_HISTORY_CACHE_MAX_SIZE);
    this.dashboardAppInfoMaxSize = this.cachedAppInfoSize;
    this.cachedAppInfoMap =
        new LinkedHashMap<String, AppInfoVO>(this.cachedAppInfoSize, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<String, AppInfoVO> eldest) {
            return size() > cachedAppInfoSize;
          }
        };
    String historyFile = this.conf.getString(CoordinatorConf.COORDINATOR_APP_HISTORY_PATH);
    if (historyFile != null && !historyFile.isEmpty()) {
      // Convert relative path to absolute path
      historyFile = convertToHadoopPath(historyFile);
      this.path = new Path(historyFile);
      this.fs =
          HadoopFilesystemProvider.getFilesystem(
              "rss_coordinator_app_history", path, this.conf.getHadoopConf());
      this.hadoopFileWriter = new HadoopFileWriter(fs, path, this.conf.getHadoopConf());
      this.batchSize = this.conf.getInteger(CoordinatorConf.COORDINATOR_APP_HISTORY_BATCH_SIZE);
      this.flushIntervalMs =
          this.conf.getLong(CoordinatorConf.COORDINATOR_APP_HISTORY_FLUSH_INTERVAL_MS);
      startPersistentThread();
      loadAppInfo();
    }
  }

  public static String convertToHadoopPath(String path) throws Exception {
    if (path.startsWith("file://./")) {
      String absPath = new File(path.substring(7)).getAbsolutePath();
      path = "file://" + absPath;
    }
    return path;
  }

  public void startPersistentThread() {
    Thread persistentThread =
        new Thread(
            () -> {
              while (isRunning) {
                try {
                  persistentAppInfoBatch();
                } catch (Exception e) {
                  LOG.error("Error in persistent thread", e);
                }
              }
            });
    persistentThread.setName("AppHistoryPersistentThread");
    persistentThread.setDaemon(true);
    persistentThread.start();
  }

  private void persistentAppInfoBatch() throws InterruptedException {
    List<AppInfoVO> batch = new ArrayList<>(batchSize);
    long startTime = System.currentTimeMillis();

    while (batch.size() < batchSize && (System.currentTimeMillis() - startTime) < flushIntervalMs) {
      AppInfoVO appInfoVO =
          appInfoQueue.poll(
              flushIntervalMs - (System.currentTimeMillis() - startTime), TimeUnit.MILLISECONDS);
      if (appInfoVO != null) {
        batch.add(appInfoVO);
      } else {
        break;
      }
    }

    if (!batch.isEmpty()) {
      persistentAppInfo(batch);
    }
  }

  public synchronized void addAppInfo(AppInfoVO appInfoVO) {
    cachedAppInfoMap.put(appInfoVO.getAppId(), appInfoVO);
    if (!appInfoQueue.offer(appInfoVO)) {
      LOG.warn(
          "add app info to queue, appId: {}, but queue is full, size: {}",
          appInfoVO.getAppId(),
          appInfoQueue.size());
    }
  }

  public void loadAppInfo() {
    try {
      FileSystem fs = path.getFileSystem(conf.getHadoopConf());
      FileStatus[] allFiles = fs.listStatus(path.getParent());

      // Filter files that match our naming pattern
      List<FileStatus> relevantFiles =
          Arrays.stream(allFiles)
              .filter(file -> file.getPath().getName().startsWith(path.getName()))
              .collect(Collectors.toList());

      // Sort relevant files in reverse order (newest first)
      relevantFiles.sort(Comparator.comparing(FileStatus::getModificationTime).reversed());

      int loadedAppInfoCount = 0;
      for (FileStatus file : relevantFiles) {
        if (loadedAppInfoCount >= cachedAppInfoSize) {
          break;
        }

        loadedAppInfoCount +=
            loadAppInfoFromFile(file.getPath(), cachedAppInfoSize - loadedAppInfoCount);
      }
    } catch (IOException e) {
      LOG.error("Failed to list or read app history files", e);
    }
  }

  private int loadAppInfoFromFile(Path filePath, int remainingCount) {
    HadoopFileReader hadoopFileReader = null;
    int loadedCount = 0;
    try {
      hadoopFileReader = new HadoopFileReader(filePath, this.conf.getHadoopConf());
      long fileSize = hadoopFileReader.getFileLen();
      long readSize = Math.min(fileSize, remainingCount * 1024L);
      long offset = Math.max(0, fileSize - readSize);

      byte[] data = hadoopFileReader.read(offset, (int) readSize);
      String dataStr = new String(data, StandardCharsets.UTF_8);
      String[] lines = dataStr.split("\n");

      for (int i = lines.length - 1; i >= 0 && loadedCount < remainingCount; i--) {
        String line = lines[i].trim();
        if (line.isEmpty()) {
          continue;
        }
        try {
          AppInfoVO appInfoVO = objectMapper.readValue(line, AppInfoVO.class);
          cachedAppInfoMap.put(appInfoVO.getAppId(), appInfoVO);
          loadedCount++;
        } catch (JsonProcessingException e) {
          //          if (i < lines.length - 1) {
          LOG.warn(
              "Skipping invalid JSON line in file {}: {}. Error: {}",
              filePath,
              line,
              e.getMessage());
          //          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to load app info from file: {}", filePath, e);
    } finally {
      if (hadoopFileReader != null) {
        try {
          hadoopFileReader.close();
        } catch (IOException e) {
          LOG.error("Failed to close HadoopFileReader for file: {}", filePath, e);
        }
      }
    }
    return loadedCount;
  }

  public void persistentAppInfo(List<AppInfoVO> appInfos) {
    try {
      for (AppInfoVO appInfoVO : appInfos) {
        String jsonString = objectMapper.writeValueAsString(appInfoVO);
        hadoopFileWriter.writeData(jsonString.getBytes(StandardCharsets.UTF_8));
        hadoopFileWriter.writeData(
            "\n"
                .getBytes(
                    StandardCharsets.UTF_8)); // Add a newline character to separate JSON objects
      }
      hadoopFileWriter.flush();
      checkAndRotateFile();
    } catch (IOException e) {
      LOG.error("write data failed, Exception: {}", e.getMessage());
    }
  }

  private void checkAndRotateFile() throws IOException {
    if (hadoopFileWriter == null) {
      LOG.warn("hadoopFileWriter is null but expected not null, reopen it path {}", path);
      hadoopFileWriter = new HadoopFileWriter(fs, path, conf.getHadoopConf());
    }
    if (hadoopFileWriter.getPos() < rotateSize) {
      return;
    }

    // Generate the new file name with the specified timestamp format
    LocalDateTime now = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    String timestamp = now.format(formatter);
    Path newPath = new Path(path.getParent(), path.getName() + "." + timestamp);

    // Rename the current file
    FileSystem fs = path.getFileSystem(conf.getHadoopConf());
    if (!fs.rename(path, newPath)) {
      throw new IOException("Failed to rename file during rotation");
    }
    // close pre handler
    hadoopFileWriter.close();
    hadoopFileWriter = null;

    // Create a new file with the original name
    hadoopFileWriter = new HadoopFileWriter(fs, path, conf.getHadoopConf());

    LOG.debug("Rotated app history file from {} to {}", path, newPath);
  }

  public void close() {
    isRunning = false;
    try {
      hadoopFileWriter.close();
    } catch (Exception e) {
      LOG.error("close failed, Exception: {}", e.getMessage());
    }
  }

  public List<AppInfoVO> getAppInfos(int currentAppSize) {
    // get the last size AppInfoVOs
    int needSize = Math.max(0, dashboardAppInfoMaxSize - currentAppSize);
    if (cachedAppInfoMap.size() < needSize) {
      return new ArrayList<>(cachedAppInfoMap.values());
    }
    return new ArrayList<>(cachedAppInfoMap.values())
        .subList(cachedAppInfoMap.size() - needSize, cachedAppInfoMap.size());
  }

  public AppInfoVO getAppInfoByUserAppId(String user, String appId) {
    if (cachedAppInfoMap.containsKey(appId)) {
      return cachedAppInfoMap.get(appId);
    }
    return null;
  }

  public static CoordinatorAppHistoryManager create(CoordinatorConf conf) {
    if (conf.getBoolean(CoordinatorConf.COORDINATOR_APP_HISTORY_ENABLE)) {
      try {
        return new CoordinatorAppHistoryManager(conf);
      } catch (Exception e) {
        LOG.error("create app history manager failed, Exception: {}", e.getMessage());
      }
    }
    return null;
  }
}
