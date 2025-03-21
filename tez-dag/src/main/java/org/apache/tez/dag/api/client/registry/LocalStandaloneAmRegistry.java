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

package org.apache.tez.dag.api.client.registry;

import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistry;
import org.apache.tez.dag.api.client.registry.zookeeper.ZkAMRegistry;
import org.apache.tez.frameworkplugins.local.LocalAmRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Optional;

public class LocalStandaloneAmRegistry extends AMRegistry {
  private String externalId;

  private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneAmRegistry.class);

  public LocalStandaloneAmRegistry(String externalId) {
    super("LocalAmRegistry");
    this.externalId = externalId;
  }

  @Override public void add(AMRecord server) throws Exception {
    if(!LocalAmRegistryClient.REGISTRY_DIR.exists()) {
      try {
        LocalAmRegistryClient.REGISTRY_DIR.mkdir();
      } catch(SecurityException e) {
        LOG.error("Unable to create directory for local registry: " + LocalAmRegistryClient.REGISTRY_DIR.getAbsolutePath());
        throw new IOException("Unable to create directory for local registry: " + LocalAmRegistryClient.REGISTRY_DIR.getAbsolutePath());
      }
    }
    File fp = new File(LocalAmRegistryClient.REGISTRY_DIR + "/" + server.getApplicationId());
    if(fp.exists()) {
      try {
        fp.delete();
      } catch(SecurityException e) {
        LOG.error("Unable to delete local registry record: " + fp.getAbsolutePath(), e);
        throw new IOException("Unable to delete local registry record: " + fp.getAbsolutePath(), e);
      }
    }
    try(PrintWriter pw =
        new PrintWriter(fp, "UTF-8")
    ) {
        RegistryUtils.ServiceRecordMarshal marshal = new RegistryUtils.ServiceRecordMarshal();
        String json = marshal.toJson(server.toServiceRecord());
        pw.println(json);
        super.add(server);
      }
  }

  @Override public void remove(AMRecord server) throws Exception {
    File fp = new File(LocalAmRegistryClient.REGISTRY_DIR + "/" + server.getApplicationId());
    try {
      fp.delete();
    } catch(SecurityException e) {
      LOG.warn("Unable to delete local registry record: " + fp.getAbsolutePath(), e);
      throw new IOException("Unable to delete local registry record: " + fp.getAbsolutePath(), e);
    }
  }

  @Override public AMRecord createAmRecord(ApplicationId appId, String hostName, int port) {
    return new AMRecord(appId, hostName, port, this.externalId);
  }

  @Override
  public Optional<ApplicationId> generateNewId()  {
    return Optional.of(ApplicationId.newInstance(System.currentTimeMillis(), 0));
  }


}
