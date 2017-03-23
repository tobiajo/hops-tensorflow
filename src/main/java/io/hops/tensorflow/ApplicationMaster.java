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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hops.tensorflow.ApplicationMasterArguments.APP_ATTEMPT_ID;
import static io.hops.tensorflow.ApplicationMasterArguments.DEBUG;
import static io.hops.tensorflow.ApplicationMasterArguments.ENV;
import static io.hops.tensorflow.ApplicationMasterArguments.HELP;
import static io.hops.tensorflow.ApplicationMasterArguments.MAIN_RELATIVE;
import static io.hops.tensorflow.ApplicationMasterArguments.MEMORY;
import static io.hops.tensorflow.ApplicationMasterArguments.PRIORITY;
import static io.hops.tensorflow.ApplicationMasterArguments.PSES;
import static io.hops.tensorflow.ApplicationMasterArguments.VCORES;
import static io.hops.tensorflow.ApplicationMasterArguments.WORKERS;
import static io.hops.tensorflow.ApplicationMasterArguments.createOptions;
import static io.hops.tensorflow.CommonArguments.ARGS;
import static io.hops.tensorflow.Constants.LOG4J_PATH;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {
  
  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  
  @VisibleForTesting
  @Private
  public enum YarnTFEvent {
    YARNTF_APP_ATTEMPT_START,
    YARNTF_APP_ATTEMPT_END,
    YARNTF_CONTAINER_START,
    YARNTF_CONTAINER_END
  }
  
  @VisibleForTesting
  @Private
  public enum YarnTFEntity {
    YARNTF_APP_ATTEMPT,
    YARNTF_CONTAINER
  }
  
  // Configuration
  private Configuration conf;
  
  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRMClient;
  
  // In both secure and non-secure modes, this points to the job-submitter.
  @VisibleForTesting
  UserGroupInformation appSubmitterUgi;
  
  // Handle to communicate with the Node Manager
  private NMClientAsync nmClientAsync;
  // Listen to process the response from the Node Manager
  private NMCallbackHandler containerListener;
  
  // Application Attempt Id ( combination of attemptId and fail count )
  @VisibleForTesting
  protected ApplicationAttemptId appAttemptID;
  
  // TODO
  // For status update for clients - yet to be implemented
  // Hostname of the container
  private String appMasterHostname;
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";
  
  private int numWorkers;
  private int numPses;
  
  // App Master configuration
  // No. of containers to run yarnTF on
  @VisibleForTesting
  protected int numTotalContainers;
  // Memory to request for the container on which the application will run
  private int containerMemory;
  // VirtualCores to request for the container on which the application will run
  private int containerVirtualCores;
  // Priority of the request
  private int requestPriority;
  
  // Counter for completed containers ( complete denotes successful or failed )
  private AtomicInteger numCompletedContainers = new AtomicInteger();
  // Allocated container count so that we know how many containers has the RM
  // allocated to us
  @VisibleForTesting
  protected AtomicInteger numAllocatedContainers = new AtomicInteger();
  // Count of failed containers
  private AtomicInteger numFailedContainers = new AtomicInteger();
  // Count of containers already requested from the RM
  // Needed as once requested, we should not request for containers again.
  // Only request for more if the original requirement changes.
  @VisibleForTesting
  protected AtomicInteger numRequestedContainers = new AtomicInteger();
  
  private String[] arguments = new String[]{};
  
  // Env variables to be setup for the yarnTF application
  private Map<String, String> environment = new HashMap<>();
  
  // Timeline domain ID
  private String domainId = null;
  
  private volatile boolean done;
  
  private ByteBuffer allTokens;
  
  // Launch threads
  private List<Thread> launchThreads = new ArrayList<>();
  
  // Timeline Client
  @VisibleForTesting
  TimelineClient timelineClient;
  
  // yarnTF stuff
  private CommandLine cliParser;
  private DistributedCacheList distCacheList;
  private String mainRelative;
  
  /**
   * @param args
   *     Command line args
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      ApplicationMaster appMaster = new ApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
      result = appMaster.finish();
    } catch (Throwable t) {
      LOG.fatal("Error running ApplicationMaster", t);
      LogManager.shutdown();
      ExitUtil.terminate(1, t);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }
  
  /**
   * Dump out contents of $CWD and the environment to stdout for debugging
   */
  private void dumpOutDebugInfo() {
    
    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val="
          + env.getValue());
    }
    
    BufferedReader buf = null;
    try {
      String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") :
          Shell.execCommand("ls", "-al");
      buf = new BufferedReader(new StringReader(lines));
      String line = "";
      while ((line = buf.readLine()) != null) {
        LOG.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.cleanup(LOG, buf);
    }
  }
  
  public ApplicationMaster() {
    // Set up the configuration
    conf = new YarnConfiguration();
  }
  
  /**
   * Parse command line options
   *
   * @param args
   *     Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   */
  public boolean init(String[] args) throws ParseException, IOException {
    Options opts = createOptions();
    cliParser = new GnuParser().parse(opts, args);
    
    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException(
          "No args specified for application master to initialize");
    }
    
    //Check whether customer log4j.properties file exists
    if (fileExist(LOG4J_PATH)) {
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class, LOG4J_PATH);
      } catch (Exception e) {
        LOG.warn("Can not set up custom log4j properties. " + e);
      }
    }
    
    if (cliParser.hasOption(HELP)) {
      printUsage(opts);
      return false;
    }
    
    if (cliParser.hasOption(DEBUG)) {
      dumpOutDebugInfo();
    }
  
    if (!cliParser.hasOption(MAIN_RELATIVE)) {
      throw new IllegalArgumentException("No main application file specified");
    }
    mainRelative = cliParser.getOptionValue(MAIN_RELATIVE);
  
    if (cliParser.hasOption(ARGS)) {
      arguments = cliParser.getOptionValues(ARGS);
    }
    
    Map<String, String> envs = System.getenv();
    
    if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption(APP_ATTEMPT_ID)) {
        String appIdStr = cliParser.getOptionValue(APP_ATTEMPT_ID, "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        throw new IllegalArgumentException(
            "Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(envs
          .get(Environment.CONTAINER_ID.name()));
      appAttemptID = containerId.getApplicationAttemptId();
    }
    
    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
      throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HOST.name())) {
      throw new RuntimeException(Environment.NM_HOST.name()
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
      throw new RuntimeException(Environment.NM_HTTP_PORT
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_PORT.name())) {
      throw new RuntimeException(Environment.NM_PORT.name()
          + " not set in the environment");
    }
    
    LOG.info("Application master for app" + ", appId="
        + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
        + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());
    
    if (cliParser.hasOption(ENV)) {
      String shellEnvs[] = cliParser.getOptionValues(ENV);
      for (String env : shellEnvs) {
        env = env.trim();
        int index = env.indexOf('=');
        if (index == -1) {
          environment.put(env, "");
          continue;
        }
        String key = env.substring(0, index);
        String val = "";
        if (index < (env.length() - 1)) {
          val = env.substring(index + 1);
        }
        environment.put(key, val);
      }
    }
    
    if (envs.containsKey(Constants.YARNTFTIMELINEDOMAIN)) {
      domainId = envs.get(Constants.YARNTFTIMELINEDOMAIN);
    }
    
    containerMemory = Integer.parseInt(cliParser.getOptionValue(MEMORY, "10"));
    containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(VCORES, "1"));
    
    numWorkers = Integer.parseInt(cliParser.getOptionValue(WORKERS, "1"));
    numPses = Integer.parseInt(cliParser.getOptionValue(PSES, "1"));
    numTotalContainers = numWorkers + numPses;
    if (numWorkers == 0 || numPses == 0) {
      throw new IllegalArgumentException("Need at least 1 worker and 1 parameter server");
    }
    requestPriority = Integer.parseInt(cliParser.getOptionValue(PRIORITY, "0"));
    return true;
  }
  
  /**
   * Helper function to print usage
   *
   * @param opts
   *     Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }
  
  /**
   * Main run function for the application master
   *
   * @throws YarnException
   * @throws IOException
   */
  @SuppressWarnings({"unchecked"})
  public void run() throws YarnException, IOException, InterruptedException {
    LOG.info("Starting ApplicationMaster. " +
        "Workers: " + numWorkers + ", Parameter servers: " + numPses);
    
    FileInputStream fin = null;
    ObjectInputStream ois = null;
    try {
      fin = new FileInputStream(Constants.DIST_CACHE_PATH);
      ois = new ObjectInputStream(fin);
      try {
        distCacheList = (DistributedCacheList) ois.readObject();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(ois);
      org.apache.commons.io.IOUtils.closeQuietly(fin);
    }
    
    LOG.info("Loaded distribute cache list: " + distCacheList.toString());
    ClusterSpecGeneratorServer clusterSpecServer = new ClusterSpecGeneratorServer(numTotalContainers);
    
    LOG.info("Starting ClusterSpecGeneratorServer");
    int port = 2222;
    while (true) {
      try {
        clusterSpecServer.start(port);
        break;
      } catch (IOException e) {
        port++;
      }
    }
    
    environment.put("AM_ADDRESS", InetAddress.getLocalHost().getHostName() + ":" + port);
    environment.put("APPLICATION_ID", appAttemptID.getApplicationId().toString());
    
    // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
    // are marked as LimitedPrivate
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    LOG.info("Executing with tokens:");
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info(token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    
    // Create appSubmitterUgi and add original tokens to it
    String appSubmitterUserName = System.getenv(Environment.USER.name());
    appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
    appSubmitterUgi.addCredentials(credentials);
    
    
    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(conf);
    amRMClient.start();
    
    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();
    
    startTimelineClient(conf);
    if (timelineClient != null) {
      publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
          YarnTFEvent.YARNTF_APP_ATTEMPT_START, domainId, appSubmitterUgi);
    }
    
    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to
    // the RPC server
    // TODO use the rpc port info to register with the RM for the client to
    // send requests to this app master
    
    // Register self with ResourceManager
    // This will start heartbeating to the RM
    appMasterHostname = NetUtils.getHostname();
    RegisterApplicationMasterResponse response = amRMClient
        .registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);
    // Dump out information about cluster capability as seen by the
    // resource manager
    int maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    
    int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);
    
    // A resource ask cannot exceed the max.
    if (containerMemory > maxMem) {
      LOG.info("Container memory specified above max threshold of cluster."
          + " Using max value." + ", specified=" + containerMemory + ", max="
          + maxMem);
      containerMemory = maxMem;
    }
    
    if (containerVirtualCores > maxVCores) {
      LOG.info("Container virtual cores specified above max threshold of cluster."
          + " Using max value." + ", specified=" + containerVirtualCores + ", max="
          + maxVCores);
      containerVirtualCores = maxVCores;
    }
    
    List<Container> previousAMRunningContainers =
        response.getContainersFromPreviousAttempts();
    LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
        + " previous attempts' running containers on AM registration.");
    numAllocatedContainers.addAndGet(previousAMRunningContainers.size());
    
    int numTotalContainersToRequest = numTotalContainers - previousAMRunningContainers.size();
    // Setup ask for containers from RM
    // Send request for containers to RM
    // Until we get our fully allocated quota, we keep on polling RM for
    // containers
    // Keep looping until all the containers are launched and Python application
    // executed on them ( regardless of success/failure).
    for (int i = 0; i < numTotalContainersToRequest; ++i) {
      ContainerRequest containerAsk = setupContainerAskForRM();
      amRMClient.addContainerRequest(containerAsk);
    }
    numRequestedContainers.set(numTotalContainers);
  }
  
  @VisibleForTesting
  void startTimelineClient(final Configuration conf)
      throws YarnException, IOException, InterruptedException {
    try {
      appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
            // Creating the Timeline Client
            timelineClient = TimelineClient.createTimelineClient();
            timelineClient.init(conf);
            timelineClient.start();
          } else {
            timelineClient = null;
            LOG.warn("Timeline service is not enabled");
          }
          return null;
        }
      });
    } catch (UndeclaredThrowableException e) {
      throw new YarnException(e.getCause());
    }
  }
  
  @VisibleForTesting
  NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler(this);
  }
  
  @VisibleForTesting
  protected boolean finish() {
    // wait for completion.
    while (!done
        && (numCompletedContainers.get() != numTotalContainers)) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {
      }
    }
    
    if (timelineClient != null) {
      publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
          YarnTFEvent.YARNTF_APP_ATTEMPT_END, domainId, appSubmitterUgi);
    }
    
    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      } catch (InterruptedException e) {
        LOG.info("Exception thrown in thread join: " + e.getMessage());
        e.printStackTrace();
      }
    }
    
    // When the application completes, it should stop all running containers
    LOG.info("Application completed. Stopping running containers");
    nmClientAsync.stop();
    
    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");
    
    FinalApplicationStatus appStatus;
    String appMessage = null;
    boolean success = true;
    if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics." + ", total=" + numTotalContainers
          + ", completed=" + numCompletedContainers.get() + ", allocated="
          + numAllocatedContainers.get() + ", failed="
          + numFailedContainers.get();
      LOG.info(appMessage);
      success = false;
    }
    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }
    
    amRMClient.stop();
    
    // Stop Timeline Client
    if (timelineClient != null) {
      timelineClient.stop();
    }
    
    return success;
  }
  
  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    
    Map<ContainerId, Container> allAllocatedContainers = new ConcurrentHashMap<>();
    
    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Got response from RM for container ask, completedCnt="
          + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info(appAttemptID + " got container status for containerID="
            + containerStatus.getContainerId() + ", state="
            + containerStatus.getState() + ", exitStatus="
            + containerStatus.getExitStatus() + ", diagnostics="
            + containerStatus.getDiagnostics());
        
        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);
        
        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          // container failed
          if (ContainerExitStatus.ABORTED != exitStatus) {
            // application failed
            // counts as completed
            numCompletedContainers.incrementAndGet();
            numFailedContainers.incrementAndGet();
          } else {
            // container was killed by framework, possibly preempted
            // we should re-try as the container was lost for some reason
            numAllocatedContainers.decrementAndGet();
            numRequestedContainers.decrementAndGet();
            // we do not need to release the container as it would be done
            // by the RM
          }
        } else {
          // nothing to do
          // container completed successfully
          numCompletedContainers.incrementAndGet();
          LOG.info("Container completed successfully." + ", containerId="
              + containerStatus.getContainerId());
        }
        if (timelineClient != null) {
          publishContainerEndEvent(
              timelineClient, containerStatus, domainId, appSubmitterUgi);
        }
      }
      
      // ask for more containers if any failed
      int askCount = numTotalContainers - numRequestedContainers.get();
      numRequestedContainers.addAndGet(askCount);
      
      if (askCount > 0) {
        for (int i = 0; i < askCount; ++i) {
          ContainerRequest containerAsk = setupContainerAskForRM();
          amRMClient.addContainerRequest(containerAsk);
        }
      }
      
      if (numCompletedContainers.get() == numTotalContainers) {
        done = true;
      }
    }
    
    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
      numAllocatedContainers.addAndGet(allocatedContainers.size());
      for (Container allocatedContainer : allocatedContainers) {
        allAllocatedContainers.put(allocatedContainer.getId(), allocatedContainer);
      }
      if (numAllocatedContainers.get() == numTotalContainers) {
        assert numAllocatedContainers.get() == allAllocatedContainers.size();
        launchAllContainers();
      }
    }
    
    private void launchAllContainers() {
      int worker = -1;
      int ps = -1;
      for (Container allocatedContainer : allAllocatedContainers.values()) {
        LOG.info("Launching yarnTF application on a new container."
            + ", containerId=" + allocatedContainer.getId()
            + ", containerNode=" + allocatedContainer.getNodeId().getHost()
            + ":" + allocatedContainer.getNodeId().getPort()
            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
            + ", containerResourceMemory"
            + allocatedContainer.getResource().getMemory()
            + ", containerResourceVirtualCores"
            + allocatedContainer.getResource().getVirtualCores());
        // + ", containerToken"
        // +allocatedContainer.getContainerToken().getIdentifier().toString());
        
        String jobName;
        int taskIndex;
        
        if (worker < numWorkers - 1) {
          jobName = "worker";
          taskIndex = ++worker;
        } else if (ps < numPses - 1) {
          jobName = "ps";
          taskIndex = ++ps;
        } else {
          throw new IllegalStateException("Too many TF tasks: worker " + worker + ", ps: " + ps);
        }
        
        LaunchContainerRunnable runnableLaunchContainer =
            new LaunchContainerRunnable(allocatedContainer, containerListener, jobName, taskIndex);
        Thread launchThread = new Thread(runnableLaunchContainer);
        
        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        launchThreads.add(launchThread);
        launchThread.start();
      }
    }
    
    @Override
    public void onShutdownRequest() {
      done = true;
    }
    
    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
    }
    
    @Override
    public float getProgress() {
      // set progress to deliver to RM on next heartbeat
      float progress = (float) numCompletedContainers.get()
          / numTotalContainers;
      return progress;
    }
    
    @Override
    public void onError(Throwable e) {
      done = true;
      amRMClient.stop();
    }
  }
  
  @VisibleForTesting
  static class NMCallbackHandler
      implements NMClientAsync.CallbackHandler {
    
    private ConcurrentMap<ContainerId, Container> containers =
        new ConcurrentHashMap<ContainerId, Container>();
    private final ApplicationMaster applicationMaster;
    
    public NMCallbackHandler(ApplicationMaster applicationMaster) {
      this.applicationMaster = applicationMaster;
    }
    
    public void addContainer(ContainerId containerId, Container container) {
      containers.putIfAbsent(containerId, container);
    }
    
    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to stop Container " + containerId);
      }
      containers.remove(containerId);
    }
    
    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: id=" + containerId + ", status=" +
            containerStatus);
      }
    }
    
    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to start Container " + containerId);
      }
      Container container = containers.get(containerId);
      if (container != null) {
        applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
      }
      if (applicationMaster.timelineClient != null) {
        ApplicationMaster.publishContainerStartEvent(
            applicationMaster.timelineClient, container,
            applicationMaster.domainId, applicationMaster.appSubmitterUgi);
      }
    }
    
    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start Container " + containerId);
      containers.remove(containerId);
      applicationMaster.numCompletedContainers.incrementAndGet();
      applicationMaster.numFailedContainers.incrementAndGet();
    }
    
    @Override
    public void onGetContainerStatusError(
        ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container " + containerId);
    }
    
    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop Container " + containerId);
      containers.remove(containerId);
    }
  }
  
  /**
   * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
   * that will execute the Python application
   */
  private class LaunchContainerRunnable implements Runnable {
    
    // Allocated container
    Container container;
    
    NMCallbackHandler containerListener;
    
    String jobName;
    int taskIndex;
    
    /**
     * @param lcontainer
     *     Allocated container
     * @param containerListener
     *     Callback handler of the container
     */
    public LaunchContainerRunnable(
        Container lcontainer, NMCallbackHandler containerListener, String jobName, int taskIndex) {
      this.container = lcontainer;
      this.containerListener = containerListener;
      this.jobName = jobName;
      this.taskIndex = taskIndex;
    }
    
    @Override
    /**
     * Connects to CM, sets up container launch context 
     * for Python application and eventually dispatches the container
     * start request to the CM. 
     */
    public void run() {
      LOG.info("Setting up container launch container for containerid="
          + container.getId());
      
      // Set the local resources
      Map<String, LocalResource> localResources = new HashMap<>();
      
      for (int i = 0; i < distCacheList.size(); i++) {
        DistributedCacheList.Entry entry = distCacheList.get(i);
        LocalResource distRsrc = LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(entry.uri),
            LocalResourceType.FILE,
            LocalResourceVisibility.APPLICATION,
            entry.size,
            entry.timestamp);
        localResources.put(entry.relativePath, distRsrc);
      }
      
      // Set the necessary command to execute on the allocated container
      Vector<CharSequence> vargs = new Vector<>(5);
      
      vargs.add("python " + mainRelative);
      
      // Set args for the Python application if any
      vargs.add(StringUtils.join(arguments, " "));
      
      // Add log redirect params
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
      
      // Get final commmand
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }
      
      List<String> commands = new ArrayList<String>();
      commands.add(command.toString());
      
      // Set up ContainerLaunchContext, setting local resource, environment,
      // command and token for constructor.
      
      // Note for tokens: Set up tokens for the container too. Today, for normal
      // Python applications, the container in yarnTF doesn't need any
      // tokens. We are populating them mainly for NodeManagers to be able to
      // download anyfiles in the distributed file-system. The tokens are
      // otherwise also useful in cases, for e.g., when one is running a
      // "hadoop dfs" command inside the distributed shell.
      Map<String, String> pyEnvCopy = new HashMap<>(environment);
      pyEnvCopy.put("JOB_NAME", jobName);
      pyEnvCopy.put("TASK_INDEX", Integer.toString(taskIndex));
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
          localResources, pyEnvCopy, commands, null, allTokens.duplicate(), null);
      containerListener.addContainer(container.getId(), container);
      nmClientAsync.startContainerAsync(container, ctx);
    }
  }
  
  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @return the setup ResourceRequest to be sent to RM
   */
  private ContainerRequest setupContainerAskForRM() {
    // setup requirements for hosts
    // using * as any host will do for the yarnTF app
    // set the priority for the request
    // TODO - what is the range for priority? how to decide?
    Priority pri = Priority.newInstance(requestPriority);
    
    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Resource.newInstance(containerMemory,
        containerVirtualCores);
    
    ContainerRequest request = new ContainerRequest(capability, null, null,
        pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }
  
  private boolean fileExist(String filePath) {
    return new File(filePath).exists();
  }
  
  private String readContent(String filePath) throws IOException {
    DataInputStream ds = null;
    try {
      ds = new DataInputStream(new FileInputStream(filePath));
      return ds.readUTF();
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(ds);
    }
  }
  
  private static void publishContainerStartEvent(
      final TimelineClient timelineClient, Container container, String domainId,
      UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getId().toString());
    entity.setEntityType(YarnTFEntity.YARNTF_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter("user", ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(YarnTFEvent.YARNTF_CONTAINER_START.toString());
    event.addEventInfo("Node", container.getNodeId().toString());
    event.addEventInfo("Resources", container.getResource().toString());
    entity.addEvent(event);
    
    try {
      ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
        @Override
        public TimelinePutResponse run() throws Exception {
          return timelineClient.putEntities(entity);
        }
      });
    } catch (Exception e) {
      LOG.error("Container start event could not be published for "
              + container.getId().toString(),
          e instanceof UndeclaredThrowableException ? e.getCause() : e);
    }
  }
  
  private static void publishContainerEndEvent(
      final TimelineClient timelineClient, ContainerStatus container,
      String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getContainerId().toString());
    entity.setEntityType(YarnTFEntity.YARNTF_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter("user", ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(YarnTFEvent.YARNTF_CONTAINER_END.toString());
    event.addEventInfo("State", container.getState().name());
    event.addEventInfo("Exit Status", container.getExitStatus());
    entity.addEvent(event);
    try {
      timelineClient.putEntities(entity);
    } catch (YarnException | IOException e) {
      LOG.error("Container end event could not be published for "
          + container.getContainerId().toString(), e);
    }
  }
  
  private static void publishApplicationAttemptEvent(
      final TimelineClient timelineClient, String appAttemptId,
      YarnTFEvent appEvent, String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(appAttemptId);
    entity.setEntityType(YarnTFEntity.YARNTF_APP_ATTEMPT.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter("user", ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setEventType(appEvent.toString());
    event.setTimestamp(System.currentTimeMillis());
    entity.addEvent(event);
    try {
      timelineClient.putEntities(entity);
    } catch (YarnException | IOException e) {
      LOG.error("App Attempt "
          + (appEvent.equals(YarnTFEvent.YARNTF_APP_ATTEMPT_START) ? "start" : "end")
          + " event could not be published for "
          + appAttemptId.toString(), e);
    }
  }
}
