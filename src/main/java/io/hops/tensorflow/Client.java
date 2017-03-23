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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static io.hops.tensorflow.ClientArguments.AM_JAR;
import static io.hops.tensorflow.ClientArguments.AM_MEMORY;
import static io.hops.tensorflow.ClientArguments.AM_PRIORITY;
import static io.hops.tensorflow.ClientArguments.AM_VCORES;
import static io.hops.tensorflow.ClientArguments.ARGS;
import static io.hops.tensorflow.ClientArguments.ATTEMPT_FAILURES_VALIDITY_INTERVAL;
import static io.hops.tensorflow.ClientArguments.CREATE;
import static io.hops.tensorflow.ClientArguments.DEBUG;
import static io.hops.tensorflow.ClientArguments.DOMAIN;
import static io.hops.tensorflow.ClientArguments.ENV;
import static io.hops.tensorflow.ClientArguments.FILES;
import static io.hops.tensorflow.ClientArguments.HELP;
import static io.hops.tensorflow.ClientArguments.KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS;
import static io.hops.tensorflow.ClientArguments.LOG_PROPERTIES;
import static io.hops.tensorflow.ClientArguments.MAIN;
import static io.hops.tensorflow.ClientArguments.MEMORY;
import static io.hops.tensorflow.ClientArguments.MODIFY_ACLS;
import static io.hops.tensorflow.ClientArguments.NAME;
import static io.hops.tensorflow.ClientArguments.NODE_LABEL_EXPRESSION;
import static io.hops.tensorflow.ClientArguments.PRIORITY;
import static io.hops.tensorflow.ClientArguments.PSES;
import static io.hops.tensorflow.ClientArguments.QUEUE;
import static io.hops.tensorflow.ClientArguments.TIMEOUT;
import static io.hops.tensorflow.ClientArguments.VCORES;
import static io.hops.tensorflow.ClientArguments.VIEW_ACLS;
import static io.hops.tensorflow.ClientArguments.WORKERS;
import static io.hops.tensorflow.ClientArguments.createOptions;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {
  
  private static final Log LOG = LogFactory.getLog(Client.class);
  
  // Configuration
  private Configuration conf;
  private YarnClient yarnClient;
  // Application master specific info to register a new Application with RM/ASM
  private String appName;
  // App master priority
  private int amPriority;
  // Queue for App master
  private String amQueue;
  // Amt. of memory resource to request for to run the App Master
  private int amMemory;
  // Amt. of virtual core resource to request for to run the App Master
  private int amVCores;
  
  // Application master jar file
  private String appMasterJar;
  // Main class to invoke application master
  private final String appMasterMainClass;
  
  // Args to be passed to the application
  private String[] arguments = new String[]{};
  // Env variables to be setup for the Python application
  private Map<String, String> environment = new HashMap<>();
  // Python application Container priority
  private int priority;
  
  // Amt of memory to request for container in which Python application will be executed
  private int memory;
  // Amt. of virtual cores to request for container in which Python application will be executed
  private int vcores;
  private String nodeLabelExpression;
  
  // log4j.properties file
  // if available, add to local resources and set into classpath
  private String log4jPropFile;
  
  // Start time for client
  private final long clientStartTime = System.currentTimeMillis();
  // Timeout threshold for client. Kill app after time interval expires.
  private long clientTimeout;
  
  // flag to indicate whether to keep containers across application attempts.
  private boolean keepContainers;
  
  private long attemptFailuresValidityInterval;
  
  // Debug flag
  boolean debugFlag;
  
  // Timeline domain ID
  private String domainId;
  
  // Flag to indicate whether to create the domain of the given ID
  private boolean toCreateDomain;
  
  // Timeline domain reader access control
  private String viewACLs;
  
  // Timeline domain writer access control
  private String modifyACLs;
  
  // Command line options
  private Options opts;
  
  private CommandLine cliParser;
  private String mainPath;
  private String mainRelativePath; // relative for worker or ps
  
  /**
   * @param args
   *     Command line arguments
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      Client client = new Client();
      LOG.info("Initializing Client");
      try {
        boolean doRun = client.init(args);
        if (!doRun) {
          System.exit(0);
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        client.printUsage();
        System.exit(-1);
      }
      result = client.run();
    } catch (Throwable t) {
      LOG.fatal("Error running Client", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application completed successfully");
      System.exit(0);
    }
    LOG.error("Application failed to complete successfully");
    System.exit(2);
  }
  
  /**
   */
  public Client(Configuration conf) throws Exception {
    this(ApplicationMaster.class.getName(), conf);
  }
  
  Client(String appMasterMainClass, Configuration conf) {
    this.conf = conf;
    this.appMasterMainClass = appMasterMainClass;
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    opts = createOptions();
  }
  
  /**
   */
  public Client() throws Exception {
    this(new YarnConfiguration());
  }
  
  /**
   * Helper function to print out usage
   */
  private void printUsage() {
    new HelpFormatter().printHelp("Client", opts);
  }
  
  /**
   * Parse command line options
   *
   * @param args
   *     Parsed command line options
   * @return Whether the init was successful to run the client
   * @throws ParseException
   */
  public boolean init(String[] args) throws ParseException {
    
    cliParser = new GnuParser().parse(opts, args);
    
    if (args.length == 0) {
      throw new IllegalArgumentException("No args specified for client to initialize");
    }
    
    if (cliParser.hasOption(LOG_PROPERTIES)) {
      String log4jPath = cliParser.getOptionValue(LOG_PROPERTIES);
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(Client.class, log4jPath);
      } catch (Exception e) {
        LOG.warn("Can not set up custom log4j properties. " + e);
      }
    }
    
    if (cliParser.hasOption(HELP)) {
      printUsage();
      return false;
    }
    
    if (cliParser.hasOption(DEBUG)) {
      debugFlag = true;
      
    }
    
    if (cliParser.hasOption(KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS)) {
      LOG.info(KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS);
      keepContainers = true;
    }
    
    appName = cliParser.getOptionValue(NAME, "yarnTF");
    amPriority = Integer.parseInt(cliParser.getOptionValue(AM_PRIORITY, "0"));
    amQueue = cliParser.getOptionValue(QUEUE, "default");
    amMemory = Integer.parseInt(cliParser.getOptionValue(AM_MEMORY, "10"));
    amVCores = Integer.parseInt(cliParser.getOptionValue(AM_VCORES, "1"));
    
    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
          + " Specified memory=" + amMemory);
    }
    if (amVCores < 0) {
      throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
          + " Specified virtual cores=" + amVCores);
    }
    
    if (!cliParser.hasOption(AM_JAR)) {
      // throw new IllegalArgumentException("No jar file specified for application master");
      appMasterJar = ApplicationMaster.class.getProtectionDomain().getCodeSource().getLocation().toString();
    } else {
      appMasterJar = cliParser.getOptionValue(AM_JAR);
    }
    
    if (!cliParser.hasOption(MAIN)) {
      throw new IllegalArgumentException("No main application file specified");
    }
    mainPath = cliParser.getOptionValue(MAIN);
    
    if (cliParser.hasOption(ARGS)) {
      arguments = cliParser.getOptionValues(ARGS);
    }
    if (cliParser.hasOption(ENV)) {
      String envs[] = cliParser.getOptionValues(ENV);
      for (String env : envs) {
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
    priority = Integer.parseInt(cliParser.getOptionValue(PRIORITY, "0"));
    
    memory = Integer.parseInt(cliParser.getOptionValue(MEMORY, "10"));
    vcores = Integer.parseInt(cliParser.getOptionValue(VCORES, "1"));
    int numWorkers = Integer.parseInt(cliParser.getOptionValue(WORKERS, "1"));
    int numPses = Integer.parseInt(cliParser.getOptionValue(PSES, "1"));
    
    if (memory < 0 || vcores < 0 || numWorkers < 1 || numPses < 1) {
      throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
          + " exiting."
          + " Specified memory=" + memory
          + ", vcores=" + vcores
          + ", numWorkers=" + numWorkers
          + ", numPses=" + numPses);
    }
    
    nodeLabelExpression = cliParser.getOptionValue(NODE_LABEL_EXPRESSION, null);
    
    clientTimeout = Integer.parseInt(cliParser.getOptionValue(TIMEOUT, "600000"));
    
    attemptFailuresValidityInterval = Long.parseLong(
        cliParser.getOptionValue(ATTEMPT_FAILURES_VALIDITY_INTERVAL, "-1"));
    
    log4jPropFile = cliParser.getOptionValue(LOG_PROPERTIES, "");
    
    // Get timeline domain options
    if (cliParser.hasOption(DOMAIN)) {
      domainId = cliParser.getOptionValue(DOMAIN);
      toCreateDomain = cliParser.hasOption(CREATE);
      if (cliParser.hasOption(VIEW_ACLS)) {
        viewACLs = cliParser.getOptionValue(VIEW_ACLS);
      }
      if (cliParser.hasOption(MODIFY_ACLS)) {
        modifyACLs = cliParser.getOptionValue(MODIFY_ACLS);
      }
    }
    
    return true;
  }
  
  /**
   * Main run function for the client
   *
   * @return true if application completed successfully
   * @throws IOException
   * @throws YarnException
   */
  public boolean run() throws IOException, YarnException {
    // Monitor the application
    return monitorApplication(submitApplication());
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  public ApplicationId submitApplication() throws IOException, YarnException {
    LOG.info("Running Client");
    yarnClient.start();
    
    logClusterState();
    
    if (domainId != null && domainId.length() > 0 && toCreateDomain) {
      prepareTimelineDomain();
    }
    
    // Get a new application id
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    ApplicationId appId = appResponse.getApplicationId();
    
    verifyClusterResources(appResponse);
    
    ContainerLaunchContext containerContext = createContainerLaunchContext(appResponse);
    ApplicationSubmissionContext appContext = createApplicationSubmissionContext(app, containerContext);
    
    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success
    // or an exception thrown to denote some form of a failure
    LOG.info("Submitting application to ASM");
    
    yarnClient.submitApplication(appContext);
    
    // TODO
    // Try submitting the same request again
    // app submission failure?
    return appId;
  }
  
  private void logClusterState() throws IOException, YarnException {
    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    LOG.info("Got Cluster metric info from ASM"
        + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());
    
    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
        NodeState.RUNNING);
    LOG.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodeReports) {
      LOG.info("Got node report from ASM for"
          + ", nodeId=" + node.getNodeId()
          + ", nodeAddress" + node.getHttpAddress()
          + ", nodeRackName" + node.getRackName()
          + ", nodeNumContainers" + node.getNumContainers());
    }
    
    QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
    LOG.info("Queue info"
        + ", queueName=" + queueInfo.getQueueName()
        + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
        + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
        + ", queueApplicationCount=" + queueInfo.getApplications().size()
        + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());
    
    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.info("User ACL Info for Queue"
            + ", queueName=" + aclInfo.getQueueName()
            + ", userAcl=" + userAcl.name());
      }
    }
  }
  
  private void verifyClusterResources(GetNewApplicationResponse appResponse) {
    // TODO get min/max resource capabilities from RM and change memory ask if needed
    // If we do not have min/max, we may not be able to correctly request
    // the required resources from the RM for the app master
    // Memory ask has to be a multiple of min and less than max.
    // Dump out information about cluster capability as seen by the resource manager
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    
    // A resource ask cannot exceed the max.
    if (amMemory > maxMem) {
      LOG.info("AM memory specified above max threshold of cluster. Using max value."
          + ", specified=" + amMemory
          + ", max=" + maxMem);
      amMemory = maxMem;
    }
    
    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);
    
    if (amVCores > maxVCores) {
      LOG.info("AM virtual cores specified above max threshold of cluster. "
          + "Using max value." + ", specified=" + amVCores
          + ", max=" + maxVCores);
      amVCores = maxVCores;
    }
  }
  
  private ContainerLaunchContext createContainerLaunchContext(GetNewApplicationResponse appResponse)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    ApplicationId appId = appResponse.getApplicationId();
    
    Map<String, String> launchEnv = setupLaunchEnv(fs, appId);
    Map<String, LocalResource> localResources = prepareLocalResources(fs, appId);
    
    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);
    
    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m");
    // Set class name
    vargs.add(appMasterMainClass);
    // Set params for Application Master
    
    vargs.add(newArg(MEMORY, String.valueOf(memory)));
    vargs.add(newArg(VCORES, String.valueOf(vcores)));
    vargs.add(newArg(PRIORITY, String.valueOf(priority)));
    
    vargs.add(newArg(ApplicationMasterArguments.MAIN_RELATIVE, mainRelativePath));
    vargs.add(newArg(ARGS, StringUtils.join(arguments, " ")));
    vargs.add(forwardArgument(WORKERS));
    vargs.add(forwardArgument(PSES));
    
    for (Map.Entry<String, String> entry : environment.entrySet()) {
      vargs.add(newArg(ENV, entry.getKey() + "=" + entry.getValue()));
    }
    if (debugFlag) {
      vargs.add("--debug");
    }
    
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");
    
    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }
    
    LOG.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
        localResources, launchEnv, commands, null, null, null);
    
    // Set the necessary security tokens as needed
    // amContainer.setContainerTokens(containerToken);
    
    // Service data is a binary blob that can be passed to the application
    // Not needed in this scenario
    // amContainer.setServiceData(serviceData);
    
    // Setup security tokens
    if (UserGroupInformation.isSecurityEnabled()) {
      // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
      Credentials credentials = new Credentials();
      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException(
            "Can't get Master Kerberos principal for the RM to use as renewer");
      }
      
      // For now, only getting tokens for the default file-system.
      final Token<?> tokens[] =
          fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          LOG.info("Got dt for " + fs.getUri() + "; " + token);
        }
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }
    
    return amContainer;
  }
  
  private String newArg(String param, String value) {
    return "--" + param + " " + value;
  }
  
  private String forwardArgument(String param) {
    return newArg(param, cliParser.getOptionValue(param));
  }
  
  private ApplicationSubmissionContext createApplicationSubmissionContext(YarnClientApplication app,
      ContainerLaunchContext containerContext) {
    // set the application name
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    
    appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
    appContext.setApplicationName(appName);
    
    if (attemptFailuresValidityInterval >= 0) {
      appContext.setAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
    }
    
    if (null != nodeLabelExpression) {
      appContext.setNodeLabelExpression(nodeLabelExpression);
    }
    
    // Set up resource type requirements
    // For now, both memory and vcores are supported, so we set memory and
    // vcores requirements
    Resource capability = Resource.newInstance(amMemory, amVCores);
    appContext.setResource(capability);
    
    appContext.setAMContainerSpec(containerContext);
    
    // Set the priority for the application master
    // TODO - what is the range for priority? how to decide?
    Priority pri = Priority.newInstance(amPriority);
    appContext.setPriority(pri);
    
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(amQueue);
    
    return appContext;
  }
  
  private Map<String, String> setupLaunchEnv(FileSystem fs, ApplicationId appId) throws IOException {
    // Set the env variables to be setup in the env where the application master will be run
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<String, String>();
    
    if (domainId != null && domainId.length() > 0) {
      env.put(Constants.YARNTFTIMELINEDOMAIN, domainId);
    }
    
    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
        "./log4j.properties");
    
    // add the runtime classpath needed for tests to work
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(':');
      classPathEnv.append(System.getProperty("java.class.path"));
    }
    
    env.put("CLASSPATH", classPathEnv.toString());
    
    return env;
  }
  
  private Map<String, LocalResource> prepareLocalResources(FileSystem fs, ApplicationId appId) throws IOException {
    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    
    LOG.info("Copy App Master jar from local filesystem and add to local environment");
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path
    addResource(fs, appId, appMasterJar, null, Constants.AM_JAR_PATH, null, localResources, null);
    
    // Set the log4j properties if needed
    if (!log4jPropFile.isEmpty()) {
      addResource(fs, appId, log4jPropFile, null, Constants.LOG4J_PATH, null, localResources, null);
    }
    
    DistributedCacheList dcl = populateDistributedCache(fs, appId);
    
    // Write distCacheList to HDFS and add to localResources
    Path baseDir = new Path(fs.getHomeDirectory(), Constants.YARNTF_STAGING + "/" + appId.toString());
    Path dclPath = new Path(baseDir, Constants.DIST_CACHE_PATH);
    FSDataOutputStream ostream = null;
    try {
      ostream = fs.create(dclPath);
      ostream.write(SerializationUtils.serialize(dcl));
    } finally {
      IOUtils.closeQuietly(ostream);
    }
    FileStatus dclStatus = fs.getFileStatus(dclPath);
    LocalResource distCacheResource = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromURI(dclPath.toUri()),
        LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION,
        dclStatus.getLen(),
        dclStatus.getModificationTime());
    localResources.put(Constants.DIST_CACHE_PATH, distCacheResource);
    
    return localResources;
  }
  
  private DistributedCacheList populateDistributedCache(FileSystem fs, ApplicationId appId) throws IOException {
    DistributedCacheList distCacheList = new DistributedCacheList();
    
    mainRelativePath = addResource(fs, appId, mainPath, null, null, distCacheList, null, null);
    
    StringBuilder pythonPath = new StringBuilder(Constants.LOCALIZED_PYTHON_DIR);
    if (cliParser.hasOption(FILES)) {
      String[] files = cliParser.getOptionValue(FILES).split(",");
      for (String file : files) {
        if (file.endsWith(".py")) {
          addResource(fs, appId, file, Constants.LOCALIZED_PYTHON_DIR, null, distCacheList, null, null);
        } else {
          addResource(fs, appId, file, null, null, distCacheList, null, pythonPath);
        }
      }
    }
    environment.put("PYTHONPATH", pythonPath.toString());
    
    return distCacheList;
  }
  
  private String addResource(FileSystem fs, ApplicationId appId, String srcPath, String dstDir, String dstName,
      DistributedCacheList distCache, Map<String, LocalResource> localResources, StringBuilder pythonPath) throws
      IOException {
    Path src = new Path(srcPath);
    
    if (dstDir == null) {
      dstDir = ".";
    }
    if (dstName == null) {
      dstName = src.getName();
    }
    
    Path baseDir = new Path(fs.getHomeDirectory(), Constants.YARNTF_STAGING + "/" + appId.toString());
    String dstPath = dstDir + "/" + dstName;
    Path dst = new Path(baseDir, dstPath);
    
    fs.copyFromLocalFile(src, dst);
    FileStatus dstStatus = fs.getFileStatus(dst);
    
    if (distCache != null) {
      distCache.add(new DistributedCacheList.Entry(
          dstPath, dst.toUri(), dstStatus.getLen(), dstStatus.getModificationTime()));
    }
    
    if (localResources != null) {
      LocalResource resource = LocalResource.newInstance(
          ConverterUtils.getYarnUrlFromURI(dst.toUri()),
          LocalResourceType.FILE,
          LocalResourceVisibility.APPLICATION,
          dstStatus.getLen(),
          dstStatus.getModificationTime());
      localResources.put(dstPath, resource);
    }
    
    if (pythonPath != null) {
      pythonPath.append(File.pathSeparator).append(dstPath);
    }
    
    return dstName;
  }
  
  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   *
   * @param appId
   *     Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  @VisibleForTesting
  @InterfaceAudience.Private
  public boolean monitorApplication(ApplicationId appId)
      throws YarnException, IOException {
    
    while (true) {
      
      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }
      
      // Get application report for the appId we are interested in 
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      
      LOG.info("Got application report from ASM for"
          + ", appId=" + appId.getId()
          + ", clientToAMToken=" + report.getClientToAMToken()
          + ", appDiagnostics=" + report.getDiagnostics()
          + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue()
          + ", appMasterRpcPort=" + report.getRpcPort()
          + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState=" + report.getYarnApplicationState().toString()
          + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
          + ", appTrackingUrl=" + report.getTrackingUrl()
          + ", appUser=" + report.getUser());
      
      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.info("Application did finished unsuccessfully."
              + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not finish."
            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
            + ". Breaking monitoring loop");
        return false;
      }
      
      if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
        LOG.info("Reached client specified timeout for application. Killing application");
        forceKillApplication(appId);
        return false;
      }
    }
    
  }
  
  /**
   * Kill a submitted application by sending a call to the ASM
   *
   * @param appId
   *     Application Id to be killed.
   * @throws YarnException
   * @throws IOException
   */
  @VisibleForTesting
  @InterfaceAudience.Private
  public void forceKillApplication(ApplicationId appId)
      throws YarnException, IOException {
    // TODO clarify whether multiple jobs with the same app id can be submitted and be running at 
    // the same time. 
    // If yes, can we kill a particular attempt only?
    
    // Response can be ignored as it is non-null on success or 
    // throws an exception in case of failures
    yarnClient.killApplication(appId);
  }
  
  private void prepareTimelineDomain() {
    TimelineClient timelineClient;
    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
      timelineClient = TimelineClient.createTimelineClient();
      timelineClient.init(conf);
      timelineClient.start();
    } else {
      LOG.warn("Cannot put the domain " + domainId +
          " because the timeline service is not enabled");
      return;
    }
    try {
      //TODO: we need to check and combine the existing timeline domain ACLs,
      //but let's do it once we have client java library to query domains.
      TimelineDomain domain = new TimelineDomain();
      domain.setId(domainId);
      domain.setReaders(viewACLs != null && viewACLs.length() > 0 ? viewACLs : " ");
      domain.setWriters(modifyACLs != null && modifyACLs.length() > 0 ? modifyACLs : " ");
      timelineClient.putDomain(domain);
      LOG.info("Put the timeline domain: " + TimelineUtils.dumpTimelineRecordtoJSON(domain));
    } catch (Exception e) {
      LOG.error("Error when putting the timeline domain", e);
    } finally {
      timelineClient.stop();
    }
  }
}
