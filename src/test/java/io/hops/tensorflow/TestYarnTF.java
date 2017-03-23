/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.tensorflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static io.hops.tensorflow.ClientArguments.AM_JAR;
import static io.hops.tensorflow.ClientArguments.AM_MEMORY;
import static io.hops.tensorflow.ClientArguments.AM_VCORES;
import static io.hops.tensorflow.ClientArguments.ARGS;
import static io.hops.tensorflow.ClientArguments.MAIN;
import static io.hops.tensorflow.ClientArguments.MEMORY;
import static io.hops.tensorflow.ClientArguments.PSES;
import static io.hops.tensorflow.ClientArguments.VCORES;
import static io.hops.tensorflow.ClientArguments.WORKERS;

public class TestYarnTF extends TestCluster {
  
  private static final Log LOG = LogFactory.getLog(TestYarnTF.class);
  
  @Test(timeout = 90000)
  public void testCreateClusterSpec() throws Exception {
    String[] args = {
        "--" + AM_JAR, APPMASTER_JAR,
        "--" + AM_MEMORY, "256",
        "--" + AM_VCORES, "1",
        "--" + MEMORY, "256",
        "--" + VCORES, "1",
        "--" + MAIN, "examples/create_cluster_spec.py",
        "--" + WORKERS, "4",
        "--" + PSES, "1",
        "--" + ARGS, "hello world"
    };
    
    LOG.info("Initializing yarnTF Client");
    final Client client = new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running yarnTF Client");
    final ApplicationId appId = client.submitApplication();
    
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(30000);
          client.forceKillApplication(appId);
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (YarnException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
        LOG.info("Kill application");
      }
    }).start();
    
    boolean result = client.monitorApplication(appId);
    LOG.info("Client run completed. Result=" + result);
    
    TestUtils.dumpAllRemoteContainersLogs(yarnCluster, appId);
    Thread.sleep(5000);
    TestUtils.dumpAllAggregatedContainersLogs(yarnCluster, appId);
  }
}
