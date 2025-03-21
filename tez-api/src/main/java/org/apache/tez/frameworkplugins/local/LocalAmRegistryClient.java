/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.frameworkplugins.local;


import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistryClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class LocalAmRegistryClient extends AMRegistryClient {

  public static final File REGISTRY_DIR = new File(
      System.getProperty("java.io.tmpdir"), "LocalAmRegistry");

  @Override public AMRecord getRecord(String appId) throws IOException {
    File fp = new File(REGISTRY_DIR.getAbsolutePath() + "/" + appId);
    StringBuilder builder = new StringBuilder();
    try(BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fp), "UTF-8"))) {
      String line = br.readLine();
      while (line != null) {
        builder.append(line);
        line = br.readLine();
      }
    }
    return AMRecord.amRecordFromServiceRecordString(builder.toString());
  }

  @Override public List<AMRecord> getAllRecords() throws IOException {
    List<AMRecord> records = new ArrayList<>();
    File[] files = REGISTRY_DIR.listFiles();
    if(files != null) {
      for (File file : files) {
        records.add(getRecord(file.getName()));
      }
    }
    return records;
  }

  @Override public void close() throws IOException {
    //NOOP
  }
}
