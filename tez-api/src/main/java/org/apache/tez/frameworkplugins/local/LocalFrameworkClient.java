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

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistryClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.frameworkplugins.ClientFrameworkService;

import java.io.IOException;
import java.util.Optional;

public class LocalFrameworkClient extends FrameworkClient {

  private ClientFrameworkService myFrameworkService;
  private AMRecord amRecord;
  private TezConfiguration tezConf;
  private AMRegistryClient amRegistryClient = null;
  private boolean isRunning = false;

  public LocalFrameworkClient(ClientFrameworkService myFrameworkService) {
    this.myFrameworkService = myFrameworkService;
  }

  @Override public void init(TezConfiguration tezConf, YarnConfiguration yarnConf) {
    this.tezConf = tezConf;
  }

  @Override public void start() {
    try {
      Optional<AMRegistryClient> registryClient =  myFrameworkService.createOrGetRegistryClient(tezConf);
      if(registryClient.isPresent()) {
        amRegistryClient = registryClient.get();
      } else {
        throw new RuntimeException("ZkAMRegistryClient is required for ZkFrameworkClient");
      }
      isRunning = true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void stop() {
    isRunning = false;
    try {
      amRegistryClient.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override public void close() throws IOException {
    amRegistryClient.close();
  }

  @Override public YarnClientApplication createApplication() throws YarnException, IOException {
    ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
    ApplicationId appId = amRecord.getApplicationId();
    context.setApplicationId(appId);
    GetNewApplicationResponse response = Records.newRecord(GetNewApplicationResponse.class);
    response.setApplicationId(appId);
    return new YarnClientApplication(response, context);
  }

  @Override public ApplicationId submitApplication(ApplicationSubmissionContext appSubmissionContext)
      throws YarnException, IOException, TezException {
    //Unused
    return null;
  }

  @Override public void killApplication(ApplicationId appId) throws YarnException, IOException {
    amRegistryClient.close();
  }

  @Override public ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException {
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setApplicationId(appId);
    report.setTrackingUrl("");
    amRecord = amRegistryClient.getRecord(appId.toString());
    report.setHost(amRecord.getHost());
    report.setRpcPort(amRecord.getPort());
    report.setYarnApplicationState(YarnApplicationState.RUNNING);
    return report;
  }

  @Override public boolean isRunning() throws IOException {
    return isRunning;
  }
}
