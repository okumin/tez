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

package org.apache.tez.test;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.common.EnvironmentUpdateUtils;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.TezDagVersionInfo;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.examples.OrderedWordCount;
import org.apache.tez.frameworkplugins.local.LocalAmRegistryClient;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tez.test.TestLocalMode.createInputFile;
import static org.junit.Assert.assertEquals;

public class TestLocalFrameworkServices {

  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")), "TestLocalFrameworkServices");

  private static final Map<String, String> standaloneEnv = new HashMap<String, String>(){
    {
      put("JVM_PID","0");
      put("NM_HOST","fakehost");
      put("NM_PORT","12");
      put("NM_HTTP_PORT","0");
      put("APP_SUBMIT_TIME_ENV",String.valueOf(System.currentTimeMillis()));
      put("USER","tez");
      put("PWD",System.getenv("PWD"));
      put("LOCAL_DIRS",System.getenv("PWD"));
      put("LOG_DIRS",System.getenv("PWD"));
      put("TEZ_EXTERNAL_ID","myTezAM");
      put("TEZ_FRAMEWORK_MODE","STANDALONE_LOCAL");
    }
  };

  static class DAGAppMasterRunnable implements Runnable {
    String[] args;
    DAGAppMasterRunnable(String ... args) {
      this.args = args;
    }

    @Override public void run() {
      DAGAppMaster.main(args);
    }
  }

  @Test
  public void testStandaloneLocalFrameworkServices() throws Exception {
    standaloneEnv.put(TezConstants.TEZ_CLIENT_VERSION_ENV, new TezDagVersionInfo().getVersion());
    EnvironmentUpdateUtils.putAll(standaloneEnv);
    DAGAppMasterRunnable runnable = new DAGAppMasterRunnable("-session");
    Thread dagThread = new Thread(runnable);
    dagThread.start();
    dagThread.join();

    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, false);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_WEBSERVICE_ENABLE, false);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_DISABLE_CLIENT_VERSION_CHECK, true);
    tezConf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    tezConf.set(TezConfiguration.TEZ_FRAMEWORK_MODE, "STANDALONE_LOCAL");
    assertEquals(verifyOrderdWorkCountJob(tezConf), DAGStatus.State.SUCCEEDED);
  }

  public DAGStatus.State verifyOrderdWorkCountJob(TezConfiguration conf) throws IOException, TezException,
      InterruptedException {
    //create inputs and outputs
    FileSystem fs = FileSystem.get(conf);
    String inputPath = new Path(TEST_DIR.getAbsolutePath(),"in").toString();
    createInputFile(fs, inputPath);
    String outputPath = new Path(TEST_DIR.getAbsolutePath(),"out").toString();
    LocalAmRegistryClient registryClientZk = new LocalAmRegistryClient();
    List<AMRecord> sessions = registryClientZk.getAllRecords();
    Collections.shuffle(sessions);
    AMRecord am = sessions.get(0);
    conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    TezClient tezClient =
        TezClient.newBuilder("TezExampleApplication", conf)
            .setIsSession(true)
            .build();
    tezClient.getClient(am.getApplicationId());
    DAG standaloneWordCount = OrderedWordCount.createDAG(conf, inputPath, outputPath, 1, true, true, "test");
    DAGClient dagClient = tezClient.submitDAG(standaloneWordCount);
    DAGStatus status = dagClient.waitForCompletion();
    return status.getState();
  }

}
