/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.manifoldcf.crawler.connectors.office365;

import java.io.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.Site;
import com.microsoft.graph.requests.extensions.*;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.http.*;
import org.apache.http.auth.Credentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.manifoldcf.agents.interfaces.*;
import org.apache.manifoldcf.crawler.system.Logging;
import org.apache.manifoldcf.connectorcommon.common.XThreadInputStream;
import org.apache.manifoldcf.connectorcommon.common.XThreadStringBuffer;
import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector;
import org.apache.manifoldcf.crawler.interfaces.*;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class Office365Connector extends BaseRepositoryConnector
{

  // to move into settings
  public final static ObjectMapper objectMapper = new ObjectMapper();

  protected IGraphServiceClient graphClient;

  /**
   * Deny access token for default authority
   */
  private final static String defaultAuthorityDenyToken = "DEAD_AUTHORITY";

  private final static String ACTION_PARAM_NAME = "action";

  private final static String ACTION_CHECK = "check";

  private final static String ACTION_SEED = "seed";

  private final static String ACTION_ITEMS = "items";

  private final static String ACTION_ITEM = "item";


  protected static final String RELATIONSHIP_RELATED = "related";

  // Template Names
  private static final String VIEW_CONFIG_FORWARD = "viewConfiguration.html";
  private static final String EDIT_CONFIG_FORWARD_HEADER = "editConfiguration.js";
  private static final String EDIT_CONFIG_FORWARD_APPLICATION_ID = "editConfiguration_ApplicationId.html";
  private static final String EDIT_CONFIG_FORWARD_ORGANIZATION_DOMAIN = "editConfiguration_OrganizationDomain.html";

  private static final String VIEW_SPEC_FORWARD = "viewSpecification.html";
  private static final String EDIT_SPEC_HEADER_FORWARD = "editSpecification.js.html";
  private static final String EDIT_SPEC_FORWARD = "editSpecification.html";

  /**
   * Constructor.
   */
  public Office365Connector()
  {
  }

  @Override
  public int getMaxDocumentRequest()
  {
    return 10;
  }

  @Override
  public String[] getRelationshipTypes()
  {
    return new String[]{RELATIONSHIP_RELATED};
  }

  @Override
  public int getConnectorModel()
  {
    return Office365Connector.MODEL_ADD_CHANGE;
  }

  /**
   * For any given document, list the bins that it is a member of.
   */
  @Override
  public String[] getBinNames(String documentIdentifier)
  {
    // Return the tenantId as the bin
    return new String[]{getConfigParameters(null).getTenantId()};
  }

  // All methods below this line will ONLY be called if a connect() call succeeded
  // on this instance!

  /**
   * Connect. The configuration parameters are included.
   *
   * @param configParams are the configuration parameters for this connection.
   *                     Note well: There are no exceptions allowed from this call, since it is
   *                     expected to mainly establish connection parameters.
   */
  @Override
  public void connect(ConfigParams configParams)
  {
    super.connect(configParams);
    if (graphClient == null) {
      graphClient = GraphServiceClient
        .builder()
        .authenticationProvider(new Office365AuthenticationProvider(getConfigParameters()))
        .buildClient();
    }
  }

  @Override
  public boolean isConnected()
  {
    return graphClient != null;
  }

  @Override
  public void disconnect()
    throws ManifoldCFException
  {
    super.disconnect();
    if (isConnected()) {
      graphClient.shutdown();
      graphClient = null;
    }
  }

  final private Office365Config getConfigParameters() { return getConfigParameters(null); }

  final private Office365Config getConfigParameters(ConfigParams configParams)
  {
    if (configParams == null)
      configParams = getConfiguration();
    return new Office365Config(configParams);
  }

  /**
   * Verifies that the settings correctly authenticate.
   *
   * @return connection result message
   */
  @Override
  public String check()
  {
    if (!isConnected()) return "Not connected.";
    Office365Config config = getConfigParameters();

    try {
      String token = ((Office365AuthenticationProvider) graphClient.getAuthenticationProvider()).getAuthToken(false);
      if (token == null || token.length() == 0) {
        return "Connection to ApplicationId failed: empty token returned.";
      }
    } catch (Exception ex) {
      return "Connection to ApplicationId failed with exception: " + ex.getMessage();
    }

    StringBuilder resultMsg = new StringBuilder();
    resultMsg.append("Connection to ApplicationId successful.\n");

    if (config.getOrganizationDomain() == null || config.getOrganizationDomain().length() == 0) {
      resultMsg.append("Organization Domain is not set.");
      return resultMsg.toString();
    }

    try {
      // Second, test if the the domain has the right permissions by accessing the root site root drive.
      Site orgDefaultSite = graphClient
        .sites(config.getOrganizationDomain())
        .buildRequest()
        .get();

      if (orgDefaultSite != null) {
        resultMsg.append("Connection to Office 365 organization domain successful.");
      } else {
        resultMsg.append("Could connect to site \"" + orgDefaultSite.displayName + "\" successful.");
      }
    }
    catch(Exception ex) {
      resultMsg.append("Failed to connect to domain with exception: " + ex.getMessage());
    }

    return resultMsg.toString();
  }

  @Override
  public String addSeedDocuments(ISeedingActivity activities, Specification spec,
                                 String lastSeedVersion, long seedTime, int jobMode)
    throws ManifoldCFException, ServiceInterruption
  {

    // Todo
    throw new NotImplementedException();
  }

  @Override
  public void processDocuments(String[] documentIdentifiers, IExistingVersions statuses, Specification spec,
                               IProcessActivity activities, int jobMode, boolean usesDefaultAuthority)
    throws ManifoldCFException, ServiceInterruption
  {

    // TODO
    throw new NotImplementedException();
  }

  @Override
  public void outputConfigurationHeader(IThreadContext threadContext, IHTTPOutput out,
                                        Locale locale, ConfigParams parameters, List<String> tabsArray)
    throws ManifoldCFException, IOException
  {
    tabsArray.add(Messages.getString(locale, "office365.ApplicationId"));
    tabsArray.add(Messages.getString(locale, "office365.OrganizationDomain"));

    Messages.outputResourceWithVelocity(out, locale, EDIT_CONFIG_FORWARD_HEADER, new HashMap<>());
  }

  @Override
  public void outputConfigurationBody(IThreadContext threadContext, IHTTPOutput out,
                                      Locale locale, ConfigParams parameters, String tabName)
    throws ManifoldCFException
  {
    HashMap<String, Object> paramMap = getConfigParameters(parameters).buildMap();
    paramMap.put("TABNAME", tabName);

    Messages.outputResourceWithVelocity(out, locale, EDIT_CONFIG_FORWARD_APPLICATION_ID, paramMap);
    Messages.outputResourceWithVelocity(out, locale, EDIT_CONFIG_FORWARD_ORGANIZATION_DOMAIN, paramMap);
  }

  @Override
  public String processConfigurationPost(IThreadContext threadContext, IPostParameters variableContext,
                                         Locale locale, ConfigParams parameters)
    throws ManifoldCFException
  {
    return Office365Config.contextToConfig(threadContext, variableContext, parameters);
  }

  @Override
  public void viewConfiguration(IThreadContext threadContext, IHTTPOutput out,
                                Locale locale, ConfigParams parameters)
    throws ManifoldCFException
  {
    HashMap<String, Object> paramMap = getConfigParameters(parameters).buildMap();
    Messages.outputResourceWithVelocity(out, locale, VIEW_CONFIG_FORWARD, paramMap);
  }

  @Override
  public void outputSpecificationHeader(IHTTPOutput out, Locale locale, Specification ds,
                                        int connectionSequenceNumber, List<String> tabsArray)
    throws ManifoldCFException, IOException
  {
    tabsArray.add(Messages.getString(locale, "office365.SharePointSites"));

    Map<String, Object> paramMap = new HashMap();
    paramMap.put("SEQNUM", Integer.toString(connectionSequenceNumber));

    Messages.outputResourceWithVelocity(out, locale, EDIT_SPEC_HEADER_FORWARD, paramMap);
  }

  @Override
  public void outputSpecificationBody(IHTTPOutput out, Locale locale, Specification ds,
                                      int connectionSequenceNumber, int actualSequenceNumber, String tabName)
    throws ManifoldCFException, IOException
  {
    HashMap<String, Object> velocityContext = getConfigParameters().buildMap();
    velocityContext.put("TABNAME", tabName);
    velocityContext.put("SEQNUM", Integer.toString(connectionSequenceNumber));
    velocityContext.put("SELECTEDNUM", Integer.toString(actualSequenceNumber));

    // Output SharePointSites tab
    fillSharePointSitesTab(velocityContext, ds);

    Messages.outputResourceWithVelocity(out, locale, EDIT_SPEC_FORWARD, velocityContext);
  }

  /** Fill in sites list */
  protected static void fillSharePointSitesTab(Map<String,Object> velocityContext, Specification ds)
  {
    List<Map<String,Object>> sites = new ArrayList();
    for (int i = 0; i < ds.getChildCount(); i++)
    {
      SpecificationNode sn = ds.getChild(i);
      if (sn.getType().equals(Office365Config.SITE_ATTR))
      {
        Map<String,Object> site = new HashMap();
        site.put("NAME_PATTERN", sn.getAttributeValue(Office365Config.SITE_NAME_PATTERN_ATTR));
        site.put("FOLDER_PATTERN", sn.getAttributeValue(Office365Config.SITE_FOLDER_PATTERN_ATTR));
        site.put("FILE_PATTERN", sn.getAttributeValue(Office365Config.SITE_FILE_PATTERN_ATTR));
        site.put("STATUS", sn.getAttributeValue(Office365Config.SITE_STATUS_ATTR));
        sites.add(site);
      }
    }

    velocityContext.put("SITES", sites);
  }

  @Override
  public String processSpecificationPost(IPostParameters variableContext, Locale locale, Specification ds,
                                         int connectionSequenceNumber)
    throws ManifoldCFException
  {
    String seqPrefix = "s" + connectionSequenceNumber + "_";
    String siteCountParam = variableContext.getParameter(seqPrefix + Office365Config.SITE_ATTR + "_count");
    if (siteCountParam != null) {
      // Delete all SITES definitions first
      int i = 0;
      while (i < ds.getChildCount()) {
        SpecificationNode sn = ds.getChild(i);
        if (sn.getType().equals(Office365Config.SITE_ATTR)) {
          ds.removeChild(i);
        } else {
          i++;
        }
      }

      int siteCount = Integer.parseInt(siteCountParam);
      i = 0;
      while (i < siteCount) {
        String sitePrefix = seqPrefix + Office365Config.SITE_ATTR + "_" + i + "_";
        String siteOpName = sitePrefix + "op";
        String opParam = variableContext.getParameter(siteOpName);
        if (opParam != null && opParam.equals("Delete")) {
          // Next row
          i++;
          continue;
        }
        SpecificationNode node = new SpecificationNode(Office365Config.SITE_ATTR);
        node.setAttribute(Office365Config.SITE_NAME_PATTERN_ATTR, variableContext.getParameter(sitePrefix + Office365Config.SITE_NAME_PATTERN_ATTR));
        node.setAttribute(Office365Config.SITE_FOLDER_PATTERN_ATTR, variableContext.getParameter(sitePrefix + Office365Config.SITE_FOLDER_PATTERN_ATTR));
        node.setAttribute(Office365Config.SITE_FILE_PATTERN_ATTR, variableContext.getParameter(sitePrefix + Office365Config.SITE_FILE_PATTERN_ATTR));
        node.setAttribute(Office365Config.SITE_STATUS_ATTR, variableContext.getParameter(sitePrefix + Office365Config.SITE_STATUS_ATTR));

        ds.addChild(ds.getChildCount(), node);
        i++;
      }

      String sitePrefix = seqPrefix + Office365Config.SITE_ATTR + "_";
      String op = variableContext.getParameter(sitePrefix + "op");
      if (op != null && op.equals("Add")) {
        SpecificationNode node = new SpecificationNode(Office365Config.SITE_ATTR);
        String siteNamePattern = variableContext.getParameter(sitePrefix + Office365Config.SITE_NAME_PATTERN_ATTR);
        node.setAttribute(Office365Config.SITE_NAME_PATTERN_ATTR, siteNamePattern);
        node.setAttribute(Office365Config.SITE_FOLDER_PATTERN_ATTR, variableContext.getParameter(sitePrefix + Office365Config.SITE_FOLDER_PATTERN_ATTR));
        node.setAttribute(Office365Config.SITE_FILE_PATTERN_ATTR, variableContext.getParameter(sitePrefix + Office365Config.SITE_FILE_PATTERN_ATTR));

        // Validate the endpoint exists when adding
        try {
          List<Site> sites = getSites(siteNamePattern);
          if (sites == null || sites.size() == 0) {
            node.setAttribute(Office365Config.SITE_STATUS_ATTR, "Site not found.");
          } else {
            node.setAttribute(Office365Config.SITE_STATUS_ATTR,
              "<ul>" +
                sites.stream().map(s -> String.format("<li>%s</li>", s.displayName)).collect(Collectors.joining()) +
              "</ul>"
            );
          }
        } catch (Exception e) {
          Logging.connectors.debug("getSites exception: " + e.getMessage());
          node.setAttribute(Office365Config.SITE_STATUS_ATTR, "Site invalid.");
        }
        ds.addChild(ds.getChildCount(), node);
      }
    }

    return null;
  }

  @Override
  public void viewSpecification(IHTTPOutput out, Locale locale, Specification ds,
                                int connectionSequenceNumber)
    throws ManifoldCFException, IOException
  {
    HashMap<String, Object> velocityContext = getConfigParameters().buildMap();
    velocityContext.put("SEQNUM", Integer.toString(connectionSequenceNumber));
    fillSharePointSitesTab(velocityContext, ds);
    Messages.outputResourceWithVelocity(out, locale, VIEW_SPEC_FORWARD, velocityContext);
  }

  private List<Site> getSites(String siteNamePattern)
    throws ManifoldCFException, ServiceInterruption
  {
    // Todo run this in a thread
    Office365Config config = getConfigParameters();
    List<Site> sites;

    FetchSitesThread sitesThread = new FetchSitesThread(graphClient, config, siteNamePattern);
    sitesThread.start();
    sites = sitesThread.finishUp();

    return sites;
  }

  protected static String[] getAcls(Specification spec)
  {
    HashMap map = new HashMap();
    int i = 0;
    while (i < spec.getChildCount()) {
      SpecificationNode sn = spec.getChild(i++);
      if (sn.getType().equals("access")) {
        String token = sn.getAttributeValue("token");
        map.put(token, token);
      }
    }

    String[] rval = new String[map.size()];
    Iterator iter = map.keySet().iterator();
    i = 0;
    while (iter.hasNext()) {
      rval[i++] = (String) iter.next();
    }
    return rval;
  }

  protected static void handleIOException(IOException e)
    throws ManifoldCFException, ServiceInterruption
  {
    if (!(e instanceof java.net.SocketTimeoutException) && (e instanceof InterruptedIOException)) {
      throw new ManifoldCFException("Interrupted: " + e.getMessage(), e, ManifoldCFException.INTERRUPTED);
    }
    long currentTime = System.currentTimeMillis();
    throw new ServiceInterruption("IO exception: " + e.getMessage(), e, currentTime + 300000L,
      currentTime + 3 * 60 * 60000L, -1, false);
  }

  static class PreemptiveAuth implements HttpRequestInterceptor
  {

    private Credentials credentials;

    public PreemptiveAuth(Credentials creds)
    {
      this.credentials = creds;
    }

    @Override
    public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException
    {
      request.addHeader(new BasicScheme(StandardCharsets.US_ASCII).authenticate(credentials, request, context));
    }
  }

  protected static class FetchSitesThread extends Thread
  {
    protected final IGraphServiceClient graphClient;
    protected final Office365Config config;
    protected final String siteNamePattern;
    protected Throwable exception = null;

    protected List<Site> sites = new ArrayList<>();

    public FetchSitesThread(IGraphServiceClient graphClient, Office365Config config, String siteNamePattern)
    {
      this.graphClient = graphClient;
      this.config = config;
      this.siteNamePattern = siteNamePattern;
      System.out.println("SiteNamePattern: " + siteNamePattern);
    }

    @Override
    public void run()
    {
      try
      {
        String siteSearch;
        if (siteNamePattern.matches("[a-zA-Z0-9\\s]*")) siteSearch = siteNamePattern;
        else siteSearch = "*";

        String siteEnumerationQuery = String.format("%s/%s/sites?search=%s",
          graphClient.getServiceRoot(), config.getOrganizationDomain(), URLEncoder.encode(siteSearch, Consts.UTF_8.name()));

        ISiteCollectionRequestBuilder scrb = (new SiteCollectionRequestBuilder(siteEnumerationQuery, graphClient, null));

        while (scrb != null) {
          ISiteCollectionPage siteCollectionPage = scrb.buildRequest().get();
          sites.addAll(siteCollectionPage.getCurrentPage());
          scrb = siteCollectionPage.getNextPage();
        }

        // Note that the search is made against display name so the patterns also should be consistent
        sites.removeIf(s -> {
          if (siteSearch.equals("*")) return !s.displayName.matches(siteNamePattern);
          else return !s.displayName.equals(siteSearch);
        });
      }
      catch (Exception ex)
      {
        exception = ex;
      }

    }

    public List<Site> finishUp()
      throws ManifoldCFException, ServiceInterruption
    {
      try {
        join();
      }
      catch (Exception ex)
      {
        exception = ex;
      }

      Throwable thr = exception;
      if (thr != null) {
        if (thr instanceof ManifoldCFException) {
          throw (ManifoldCFException) thr;
        } else if (thr instanceof ServiceInterruption || thr instanceof InterruptedException) {
          throw (ServiceInterruption) thr;
        }
        throw new ManifoldCFException("getSites error: " + thr.getMessage(), thr);
      }

      return this.sites;
    }
  }

  protected static class ExecuteSeedingThread extends Thread
  {

    protected final HttpClient client;

    protected final String url;

    protected final XThreadStringBuffer seedBuffer;

    protected Throwable exception = null;

    public ExecuteSeedingThread(HttpClient client, String url)
    {
      super();
      setDaemon(true);
      this.client = client;
      this.url = url;
      seedBuffer = new XThreadStringBuffer();
    }

    public XThreadStringBuffer getBuffer()
    {
      return seedBuffer;
    }

    public void finishUp()
      throws InterruptedException
    {
      seedBuffer.abandon();
      join();
      Throwable thr = exception;
      if (thr != null) {
        if (thr instanceof RuntimeException) {
          throw (RuntimeException) thr;
        } else if (thr instanceof Error) {
          throw (Error) thr;
        } else {
          throw new RuntimeException("Unhandled exception of type: " + thr.getClass().getName(), thr);
        }
      }
    }

    @Override
    public void run()
    {
      HttpGet method = new HttpGet(url.toString());

      try {
        HttpResponse response = client.execute(method);
        try {
          if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            exception = new ManifoldCFException("addSeedDocuments error - interface returned incorrect return code for: " + url + " - " + response.getStatusLine().toString());
            return;
          }

          try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            SAXParser parser = factory.newSAXParser();
            DefaultHandler handler = new SAXSeedingHandler(seedBuffer);
            parser.parse(response.getEntity().getContent(), handler);
          } catch (FactoryConfigurationError ex) {
            exception = new ManifoldCFException("addSeedDocuments error: " + ex.getMessage(), ex);
          } catch (ParserConfigurationException ex) {
            exception = new ManifoldCFException("addSeedDocuments error: " + ex.getMessage(), ex);
          } catch (SAXException ex) {
            exception = new ManifoldCFException("addSeedDocuments error: " + ex.getMessage(), ex);
          }
          seedBuffer.signalDone();
        } finally {
          EntityUtils.consume(response.getEntity());
          method.releaseConnection();
        }
      } catch (IOException ex) {
        exception = ex;
      }
    }

    public Throwable getException()
    {
      return exception;
    }
  }

  protected static class DocumentVersionThread extends Thread
  {

    protected final HttpClient client;

    protected final String url;

    protected Throwable exception = null;

    protected final String[] versions;

    protected final String[] documentIdentifiers;

    protected final String genericAuthMode;

    protected final String defaultRights;

    public DocumentVersionThread(HttpClient client, String url, String[] documentIdentifiers, String genericAuthMode, String defaultRights)
    {
      super();
      setDaemon(true);
      this.client = client;
      this.url = url;
      this.documentIdentifiers = documentIdentifiers;
      this.genericAuthMode = genericAuthMode;
      this.defaultRights = defaultRights;
      this.versions = new String[documentIdentifiers.length];
      for (int i = 0; i < versions.length; i++) {
        versions[i] = null;
      }
    }

    @Override
    public void run()
    {
      try {
        HttpGet method = new HttpGet(url.toString());

        HttpResponse response = client.execute(method);
        try {
          if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            exception = new ManifoldCFException("addSeedDocuments error - interface returned incorrect return code for: " + url + " - " + response.getStatusLine().toString());
            return;
          }

          // TODO

        } finally {
          EntityUtils.consume(response.getEntity());
          method.releaseConnection();
        }
      } catch (Exception ex) {
        exception = ex;
      }
    }

    public String[] finishUp()
      throws ManifoldCFException, ServiceInterruption, IOException, InterruptedException
    {
      join();
      Throwable thr = exception;
      if (thr != null) {
        if (thr instanceof ManifoldCFException) {
          throw (ManifoldCFException) thr;
        } else if (thr instanceof ServiceInterruption) {
          throw (ServiceInterruption) thr;
        } else if (thr instanceof IOException) {
          throw (IOException) thr;
        } else if (thr instanceof RuntimeException) {
          throw (RuntimeException) thr;
        } else if (thr instanceof Error) {
          throw (Error) thr;
        }
        throw new ManifoldCFException("getDocumentVersions error: " + thr.getMessage(), thr);
      }
      return versions;
    }
  }

  protected static class ExecuteProcessThread extends Thread
  {

    protected final HttpClient client;

    protected final String url;

    protected Throwable exception = null;

    protected XThreadInputStream threadStream;

    protected boolean abortThread = false;

    protected long streamLength = 0;

    public ExecuteProcessThread(HttpClient client, String url)
    {
      super();
      setDaemon(true);
      this.client = client;
      this.url = url;
    }

    @Override
    public void run()
    {
      try {
        HttpGet method = new HttpGet(url);
        HttpResponse response = client.execute(method);
        try {
          if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            exception = new ManifoldCFException("processDocuments error - interface returned incorrect return code for: " + url + " - " + response.getStatusLine().toString());
            return;
          }
          synchronized (this) {
            if (!abortThread) {
              streamLength = response.getEntity().getContentLength();
              threadStream = new XThreadInputStream(response.getEntity().getContent());
              this.notifyAll();
            }
          }

          if (threadStream != null) {
            // Stuff the content until we are done
            threadStream.stuffQueue();
          }
        } catch (Throwable ex) {
          exception = ex;
        } finally {
          EntityUtils.consume(response.getEntity());
          method.releaseConnection();
        }
      } catch (Throwable e) {
        exception = e;
      }
    }

    public InputStream getSafeInputStream() throws InterruptedException, IOException, ManifoldCFException
    {
      while (true) {
        synchronized (this) {
          if (exception != null) {
            throw new IllegalStateException("Check for response before getting stream");
          }
          checkException(exception);
          if (threadStream != null) {
            return threadStream;
          }
          wait();
        }
      }
    }

    public long getStreamLength() throws IOException, InterruptedException, ManifoldCFException
    {
      while (true) {
        synchronized (this) {
          if (exception != null) {
            throw new IllegalStateException("Check for response before getting stream");
          }
          checkException(exception);
          if (threadStream != null) {
            return streamLength;
          }
          wait();
        }
      }
    }

    protected synchronized void checkException(Throwable exception)
      throws IOException, ManifoldCFException
    {
      if (exception != null) {
        Throwable e = exception;
        if (e instanceof IOException) {
          throw (IOException) e;
        } else if (e instanceof ManifoldCFException) {
          throw (ManifoldCFException) e;
        } else if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else if (e instanceof Error) {
          throw (Error) e;
        } else {
          throw new RuntimeException("Unhandled exception of type: " + e.getClass().getName(), e);
        }
      }
    }

    public void finishUp()
      throws InterruptedException, IOException, ManifoldCFException
    {
      // This will be called during the finally
      // block in the case where all is well (and
      // the stream completed) and in the case where
      // there were exceptions.
      synchronized (this) {
        if (threadStream != null) {
          threadStream.abort();
        }
        abortThread = true;
      }
      join();
      checkException(exception);
    }

    public Throwable getException()
    {
      return exception;
    }
  }

  static public class SAXSeedingHandler extends DefaultHandler
  {

    protected XThreadStringBuffer seedBuffer;

    public SAXSeedingHandler(XThreadStringBuffer seedBuffer)
    {
      this.seedBuffer = seedBuffer;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException
    {
      if ("seed".equals(localName) && attributes.getValue("id") != null) {
        try {
          seedBuffer.add(attributes.getValue("id"));
        } catch (InterruptedException ex) {
          throw new SAXException("Adding seed failed: " + ex.getMessage(), ex);
        }
      }
    }
  }
}
