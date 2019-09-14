/* $Id: ReDockConnector.java 988245 2010-08-23 18:39:35Z kwright $ */

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.manifoldcf.agents.output.redock;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpHeaders;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.manifoldcf.agents.output.BaseOutputConnector;
import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.agents.interfaces.*;

import java.util.*;
import java.io.*;

/** This is a reDock output connector that sends the document to a reDock endpoint.
 */
public class ReDockConnector extends BaseOutputConnector {
  // Activities we log

  /** Ingestion activity */
  public final static String INGEST_ACTIVITY = "reDock ingest";
  /** Document removal activity */
  public final static String REMOVE_ACTIVITY = "reDock deletion";
  /** Job notify activity */
  public final static String JOB_COMPLETE_ACTIVITY = "output notification";

  private final static String REDOCK_TAB_SERVER = "reDockConnector.EndPoint";

  /** Forward to the javascript to check the configuration parameters */
  private static final String EDIT_CONFIG_HEADER_FORWARD = "editConfiguration.js";

  /** Forward to the HTML template to edit the configuration parameters */
  private static final String EDIT_CONFIG_FORWARD_SERVER = "editConfiguration_Server.html";

  /** Forward to the HTML template to view the configuration parameters */
  private static final String VIEW_CONFIG_FORWARD = "viewConfiguration.html";

  /** Connection expiration interval */
  private static final long EXPIRATION_INTERVAL = 60000L;

  private HttpClientConnectionManager connectionManager = null;
  private HttpClient client = null;
  private long expirationTime = -1L;


  /** Constructor.
   */
  public ReDockConnector() {
  }

  /** Return the list of activities that this connector supports (i.e. writes into the log).
   *@return the list.
   */
  @Override
  public String[] getActivitiesList() {
    return new String[]{INGEST_ACTIVITY, REMOVE_ACTIVITY, JOB_COMPLETE_ACTIVITY};
  }

  /** Connect.
   *@param configParams is the set of configuration parameters, which
   * in this case describe the target appliance, basic auth configuration, etc.  (This formerly came
   * out of the ini file.)
   */
  @Override
  public void connect(ConfigParams configParams) {
    super.connect(configParams);
  }

  protected HttpClient getSession() {
    if (client == null) {
      int socketTimeout = 900000;
      int connectionTimeout = 60000;

      // Load configuration from parameters
      final ReDockConfig config = new ReDockConfig(params);

      // Set up connection manager
      PoolingHttpClientConnectionManager poolingConnectionManager = new PoolingHttpClientConnectionManager(RegistryBuilder.<ConnectionSocketFactory>create()
        .register("http", PlainConnectionSocketFactory.getSocketFactory())
        .register("https",  SSLConnectionSocketFactory.getSocketFactory())
        .build());
      poolingConnectionManager.setDefaultMaxPerRoute(1);
      poolingConnectionManager.setValidateAfterInactivity(2000);
      poolingConnectionManager.setDefaultSocketConfig(SocketConfig.custom()
        .setTcpNoDelay(true)
        .setSoTimeout(socketTimeout)
        .build());
      connectionManager = poolingConnectionManager;

      RequestConfig.Builder requestBuilder = RequestConfig.custom()
        .setCircularRedirectsAllowed(true)
        .setSocketTimeout(socketTimeout)
        .setExpectContinueEnabled(true)
        .setConnectTimeout(connectionTimeout)
        .setConnectionRequestTimeout(socketTimeout);

      client = HttpClients.custom()
        .setConnectionManager(connectionManager)
        .setMaxConnTotal(1)
        .disableAutomaticRetries()
        .setDefaultRequestConfig(requestBuilder.build())
        .setRequestExecutor(new HttpRequestExecutor(socketTimeout))
        // Provide
        .setDefaultHeaders(Arrays.asList(
          new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + config.getToken()),
          new BasicHeader("X-RedockAuthMode", "internal_token")
        ))
        .build();
    }

    expirationTime = System.currentTimeMillis() + EXPIRATION_INTERVAL;
    return client;
  }

  protected void closeSession() {
    if (connectionManager != null) {
      connectionManager.shutdown();
      connectionManager = null;
    }
    client = null;
    expirationTime = -1L;
  }

  /** This method is called to assess whether to count this connector instance should
   * actually be counted as being connected.
   *@return true if the connector instance is actually connected.
   */
  @Override
  public boolean isConnected() {
    return connectionManager != null;
  }

  @Override
  public void disconnect()
    throws ManifoldCFException {
    super.disconnect();
    closeSession();
  }

  @Override
  public void poll()
    throws ManifoldCFException {
    super.poll();
    if (connectionManager != null) {
      if (System.currentTimeMillis() > expirationTime) {
        closeSession();
      }
    }
  }

  /** Get an output version string, given an output specification.  The output version string is used to uniquely describe the pertinent details of
   * the output specification and the configuration, to allow the Connector Framework to determine whether a document will need to be output again.
   * Note that the contents of the document cannot be considered by this method, and that a different version string (defined in IRepositoryConnector)
   * is used to describe the version of the actual document.
   *
   * This method presumes that the connector object has been configured, and it is thus able to communicate with the output data store should that be
   * necessary.
   *@param spec is the current output specification for the job that is doing the crawling.
   *@return a string, of unlimited length, which uniquely describes output configuration and specification in such a way that if two such strings are equal,
   * the document will not need to be sent again to the output data store.
   */
  @Override
  public VersionContext getPipelineDescription(Specification spec) {
    return new VersionContext("", params, spec);
  }

  /** Add (or replace) a document in the output data store using the connector.
   * This method presumes that the connector object has been configured, and it is thus able to communicate with the output data store should that be
   * necessary.
   * The OutputSpecification is *not* provided to this method, because the goal is consistency, and if output is done it must be consistent with the
   * output description, since that was what was partly used to determine if output should be taking place.  So it may be necessary for this method to decode
   * an output description string in order to determine what should be done.
   *@param documentURI is the URI of the document.  The URI is presumed to be the unique identifier which the output data store will use to process
   * and serve the document.  This URI is constructed by the repository connector which fetches the document, and is thus universal across all output connectors.
   *@param outputDescription is the description string that was constructed for this document by the getOutputDescription() method.
   *@param document is the document data to be processed (handed to the output data store).
   *@param authorityNameString is the name of the authority responsible for authorizing any access tokens passed in with the repository document.  May be null.
   *@param activities is the handle to an object that the implementer of an output connector may use to perform operations, such as logging processing activity.
   *@return the document status (accepted or permanently rejected).
   */
  @Override
  public int addOrReplaceDocumentWithException(String documentURI, VersionContext outputDescription, RepositoryDocument document, String authorityNameString, IOutputAddActivity activities)
    throws ManifoldCFException, ServiceInterruption, IOException {

    // Reject temporary working documents that starts with ~
    if (document.getFileName().startsWith("~")) {
      return DOCUMENTSTATUS_REJECTED;
    }

    // Establish a session
    HttpClient client = getSession();
    ReDockConfig config = getConfigParameters(null);
    ReDockAction reDock = new ReDockAction(client, config);

    try
    {
      reDock.executePUT(documentURI, document);

      if (reDock.getResult() != ReDockConnection.Result.OK) {
        return DOCUMENTSTATUS_REJECTED;
      }

      activities.recordActivity(null, INGEST_ACTIVITY, document.getBinaryLength(), documentURI, "OK", reDock.getResultDescription());
    }
    catch(Exception e)
    {
      activities.recordActivity(null, INGEST_ACTIVITY, document.getBinaryLength(), documentURI, "EXCEPTION", e.toString());
    }

    return DOCUMENTSTATUS_ACCEPTED;
  }

  private final static Set<String> acceptableMimeTypes = new HashSet<String>();
  static
  {
    acceptableMimeTypes.add("application/msword"); // .doc, .dot
    acceptableMimeTypes.add("application/pdf");
    acceptableMimeTypes.add("application/vnd.openxmlformats-officedocument.wordprocessingml.document"); // .docx
    acceptableMimeTypes.add("application/vnd.ms-word.document.macroenabled.12"); // .docm
    acceptableMimeTypes.add("application/vnd.ms-word.template.macroenabled.12"); // .dotm
    acceptableMimeTypes.add("application/vnd.ms-powerpoint"); // .ppt, .pot, .pps, .ppa
    acceptableMimeTypes.add("application/vnd.openxmlformats-officedocument.presentationml.presentation"); // .pptx
    acceptableMimeTypes.add("application/vnd.openxmlformats-officedocument.presentationml.template"); // .potx
    acceptableMimeTypes.add("application/vnd.openxmlformats-officedocument.presentationml.slideshow"); // ppsx
    acceptableMimeTypes.add("application/vnd.ms-powerpoint.presentation.macroenabled.12"); //.pptm
    acceptableMimeTypes.add("application/vnd.ms-powerpoint.addin.macroenabled.12"); // .ppam
    acceptableMimeTypes.add("application/vnd.ms-powerpoint.slideshow.macroenabled.12"); // .ppsm
  }

  /** Detect if a mime type is indexable or not.  This method is used by participating repository connectors to pre-filter the number of
   * unusable documents that will be passed to this output connector.
   *@param outputDescription is the document's output version.
   *@param mimeType is the mime type of the document.
   *@return true if the mime type is indexable by this connector.
   */
  @Override
  public boolean checkMimeTypeIndexable(VersionContext outputDescription, String mimeType, IOutputCheckActivity activities)
  {
    if (mimeType == null) {
      return false;
    }
    return acceptableMimeTypes.contains(mimeType.toLowerCase(Locale.ROOT));
  }

  /** Remove a document using the connector.
   * Note that the last outputDescription is included, since it may be necessary for the connector to use such information to know how to properly remove the document.
   *@param documentURI is the URI of the document.  The URI is presumed to be the unique identifier which the output data store will use to process
   * and serve the document.  This URI is constructed by the repository connector which fetches the document, and is thus universal across all output connectors.
   *@param outputDescription is the last description string that was constructed for this document by the getOutputDescription() method above.
   *@param activities is the handle to an object that the implementer of an output connector may use to perform operations, such as logging processing activity.
   */
  @Override
  public void removeDocument(String documentURI, String outputDescription, IOutputRemoveActivity activities)
    throws ManifoldCFException, ServiceInterruption {
    // Establish a session
    HttpClient client = getSession();
    ReDockConfig config = getConfigParameters(null);
    ReDockAction reDock = new ReDockAction(client, config);

    try
    {
      reDock.executeDELETE(documentURI);
    }
    finally
    {
      activities.recordActivity(null, REMOVE_ACTIVITY, null, documentURI, reDock.getResult().toString(), null);
    }
  }

  /** Notify the connector of a completed job.
   * This is meant to allow the connector to flush any internal data structures it has been keeping around, or to tell the output repository that this
   * is a good time to synchronize things.  It is called whenever a job is either completed or aborted.
   *@param activities is the handle to an object that the implementer of an output connector may use to perform operations, such as logging processing activity.
   */
  @Override
  public void noteJobComplete(IOutputNotifyActivity activities)
    throws ManifoldCFException {
    activities.recordActivity(null, JOB_COMPLETE_ACTIVITY, null, "", "OK", null);
  }

  /** Read the content of a resource, replace the variable ${PARAMNAME} with the
   * value and copy it to the out.
   *
   * @param resName
   * @param out
   * @throws ManifoldCFException */
  private static void outputResource(String resName, IHTTPOutput out,
                                     Locale locale, ReDockParam params,
                                     String tabName, Integer sequenceNumber, Integer currentSequenceNumber) throws ManifoldCFException {
    Map<String, Object> paramMap = null;
    if (params != null) {
      paramMap = params.buildMap(out);
      if (tabName != null) {
        paramMap.put("TabName", tabName);
      }
      if (currentSequenceNumber != null)
        paramMap.put("SelectedNum", currentSequenceNumber.toString());
    } else {
      paramMap = new HashMap<String, Object>();
    }
    if (sequenceNumber != null)
      paramMap.put("SeqNum", sequenceNumber.toString());

    Messages.outputResourceWithVelocity(out, locale, resName, paramMap);
  }

  @Override
  public void outputConfigurationHeader(IThreadContext threadContext,
                                        IHTTPOutput out, Locale locale, ConfigParams parameters,
                                        List<String> tabsArray) throws ManifoldCFException, IOException {
    super.outputConfigurationHeader(threadContext, out, locale, parameters, tabsArray);
    tabsArray.add(Messages.getString(locale, REDOCK_TAB_SERVER));
    outputResource(EDIT_CONFIG_HEADER_FORWARD, out, locale, null, null, null, null);
  }

  @Override
  public void outputConfigurationBody(IThreadContext threadContext,
                                      IHTTPOutput out, Locale locale, ConfigParams parameters, String tabName)
    throws ManifoldCFException, IOException {
    try {
      super.outputConfigurationBody(threadContext, out, locale, parameters, tabName);
      ReDockConfig config = this.getConfigParameters(parameters);
      outputResource(EDIT_CONFIG_FORWARD_SERVER, out, locale, config, tabName, null, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** Build a Set of reDock parameters. If configParams is null,
   * getConfiguration() is used.
   *
   * @param configParams */
  final private ReDockConfig getConfigParameters(
    ConfigParams configParams) {
    if (configParams == null)
      configParams = getConfiguration();
    return new ReDockConfig(configParams);
  }

  @Override
  public void viewConfiguration(IThreadContext threadContext, IHTTPOutput out,
                                Locale locale, ConfigParams parameters)
    throws ManifoldCFException {
    outputResource(VIEW_CONFIG_FORWARD, out, locale,
      getConfigParameters(parameters), null, null, null);
  }

  @Override
  public String processConfigurationPost(IThreadContext threadContext,
                                         IPostParameters variableContext, ConfigParams parameters)
    throws ManifoldCFException {
    return ReDockConfig.contextToConfig(threadContext, variableContext, parameters);
  }

  /** Test the connection.  Returns a string describing the connection integrity.
   *@return the connection's status as a displayable string.
   */
  @Override
  public String check() throws ManifoldCFException {
    HttpClient client = getSession();
    ReDockAction reDock = new ReDockAction(client, getConfigParameters(null));
    try {
      reDock.executeGET("check");
      String resultName = reDock.getResult().name();
      if (resultName.equals("OK")) {
        JsonNode responseNode = reDock.getResponseJsonNode();
        String clientName = responseNode.findValue("client").asText();
        String environment = responseNode.findValue("environment").asText();
        return super.check() + " with ClientName = " + clientName + ", and Environment = " + environment + ".";
      }
      return resultName + " " + reDock.getResultDescription();
    } catch (ServiceInterruption e) {
      return "Transient exception: " + e.getMessage();
    }
  }
}
