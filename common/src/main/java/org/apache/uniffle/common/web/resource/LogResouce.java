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

package org.apache.uniffle.common.web.resource;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hbase.thirdparty.javax.ws.rs.DefaultValue;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.PathParam;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.QueryParam;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response;
import org.apache.hbase.thirdparty.javax.ws.rs.core.StreamingOutput;

@Path("/logs/{file}")
public class LogResouce {

  private static final String LOG_DIR = System.getenv("RSS_LOG_DIR");

  public static Map<String, Long> getLogList() {
    if (LOG_DIR == null || LOG_DIR.isEmpty()) {
      return Collections.singletonMap("Error", -1L);
    }

    try (Stream<java.nio.file.Path> pathStream = Files.list(Paths.get(LOG_DIR))) {
      return pathStream
          .filter(Files::isRegularFile)
          .collect(
              Collectors.toMap(
                  path -> path.getFileName().toString(),
                  path -> {
                    try {
                      return Files.size(path);
                    } catch (IOException e) {
                      return -1L;
                    }
                  }));
    } catch (IOException e) {
      return Collections.singletonMap("Error", -1L);
    }
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response getLog(
      @PathParam("file") String fileName,
      @QueryParam("offset") @DefaultValue("0") long offset,
      @QueryParam("size") @DefaultValue("1048576") int size) {
    String logFilePath = LOG_DIR + File.separator + fileName;
    File logFile = new File(logFilePath);
    if (!logFile.exists()) {
      return Response.status(Response.Status.NOT_FOUND).entity("Log file not found").build();
    }

    StreamingOutput streamingOutput =
        output -> {
          try (RandomAccessFile raf = new RandomAccessFile(logFile, "r")) {
            long fileLength = raf.length();
            long startOffset = Math.max(0, offset - 1024);
            long endOffset = Math.min(fileLength, offset + size + 1024);

            raf.seek(startOffset);
            byte[] buffer = new byte[4096];
            int bytesRead;
            boolean firstLine = true;
            boolean lastLine = false;

            while ((bytesRead = raf.read(buffer)) != -1) {
              if (firstLine) {
                int newLineIndex = indexOf(buffer, (byte) '\n', 0, bytesRead);
                if (newLineIndex != -1) {
                  output.write(buffer, newLineIndex + 1, bytesRead - newLineIndex - 1);
                  firstLine = false;
                }
              } else if (lastLine) {
                int newLineIndex = lastIndexOf(buffer, (byte) '\n', 0, bytesRead);
                if (newLineIndex != -1) {
                  output.write(buffer, 0, newLineIndex + 1);
                }
                break;
              } else {
                output.write(buffer, 0, bytesRead);
              }

              if (raf.getFilePointer() >= endOffset) {
                lastLine = true;
              }
            }
            output.flush();
          }
        };

    return Response.ok(streamingOutput).build();
  }

  private int indexOf(byte[] array, byte target, int start, int end) {
    for (int i = start; i < end; i++) {
      if (array[i] == target) {
        return i;
      }
    }
    return -1;
  }

  private int lastIndexOf(byte[] array, byte target, int start, int end) {
    for (int i = end - 1; i >= start; i--) {
      if (array[i] == target) {
        return i;
      }
    }
    return -1;
  }
}
