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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.Site;
import org.apache.commons.lang.StringUtils;
import org.apache.manifoldcf.agents.interfaces.RepositoryDocument;
import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.connectorcommon.common.XThreadInputStream;
import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.core.util.URLEncoder;
import org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector;
import org.apache.manifoldcf.crawler.interfaces.IExistingVersions;
import org.apache.manifoldcf.crawler.interfaces.IProcessActivity;
import org.apache.manifoldcf.crawler.interfaces.ISeedingActivity;
import org.apache.manifoldcf.crawler.system.Logging;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Office365Connector extends BaseRepositoryConnector
{

  // to move into settings
  public final static ObjectMapper objectMapper = new ObjectMapper();

  protected Office365Session session;

  private final static String ACTIVITY_READ = "read document";

  private final static String ACTIVITY_FETCH = "fetch";

  // Template Names
  private static final String VIEW_CONFIG_FORWARD = "viewConfiguration.html";
  private static final String EDIT_CONFIG_FORWARD_HEADER = "editConfiguration.js";
  private static final String EDIT_CONFIG_FORWARD_APPLICATION_ID = "editConfiguration_ApplicationId.html";
  private static final String EDIT_CONFIG_FORWARD_ORGANIZATION_DOMAIN = "editConfiguration_OrganizationDomain.html";

  private static final String VIEW_SPEC_FORWARD = "viewSpecification.html";
  private static final String EDIT_SPEC_HEADER_FORWARD = "editSpecification.js.html";
  private static final String EDIT_SPEC_FORWARD = "editSpecification.html";

  // Regex
  private static final Pattern DOMAIN_PATTERN = Pattern.compile("https://(.*?)/");
  private static final Pattern SITE_PATTERN = Pattern.compile("sites/(.*?)/");

  /**
   * Constructor.
   */
  public Office365Connector()
  {
    super();
  }

  @Override
  public int getMaxDocumentRequest()
  {
    return 10;
  }

  @Override
  public String[] getRelationshipTypes()
  {
    return new String[]{};
  }

  @Override
  public int getConnectorModel()
  {
    return Office365Connector.MODEL_ADD_CHANGE_DELETE;
  }

  @Override
  public String[] getActivitiesList() {
    return new String[]{ACTIVITY_FETCH, ACTIVITY_READ};
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
    Office365Config config = getConfigParameters(configParams);

    if (session == null) {
      session = new Office365Session(config);
    }
  }

  @Override
  public boolean isConnected()
  {
    return session != null;
  }

  @Override
  public void disconnect()
    throws ManifoldCFException
  {
    super.disconnect();
    if (isConnected()) {
      session.close();
      session = null;
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

    return session.check();
  }

  /**
   * Using the SITE.NAME_PATTERN from the SITES spec, retrieve all the unique Drives.  Seed them using Microsoft
   * drive/root/delta incremental API (https://docs.microsoft.com/en-us/onedrive/developer/rest-api/concepts/scan-guidance?view=odsp-graph-online)
   * and
   * @param activities is the interface this method should use to perform whatever framework actions are desired.
   * @param spec is a document specification (that comes from the job).
   * @param lastSeedVersion
   * @param seedTime is the end of the time range of documents to consider, exclusive.
   * @param jobMode is an integer describing how the job is being run, whether continuous or once-only.
   * @return
   * @throws ManifoldCFException
   * @throws ServiceInterruption
   */
  @Override
  public String addSeedDocuments(ISeedingActivity activities, Specification spec,
                                 String lastSeedVersion, long seedTime, int jobMode)
    throws ManifoldCFException, ServiceInterruption
  {
    Office365Config config = getConfigParameters();
    Set<String> fetchedSites = new HashSet<>();
    HashMap<String, String> sitesToDelta = new HashMap();

    // Extract the site to delta url from previous seeding
    if (lastSeedVersion != null) {
      for (String siteDeltaPair : lastSeedVersion.split(";;")) {
        String[] siteDeltaTokens = siteDeltaPair.split("::");
        sitesToDelta.put(siteDeltaTokens[0], siteDeltaTokens[1]);
      }
    }

    try {
      // Build Drive path based on sites that match the sites name patterns. Redo it at every seeding as new sites may have been added
      for (int i = 0; i < spec.getChildCount(); i++) {
        SpecificationNode sn = spec.getChild(i);
        if (sn.getType().equals(Office365Config.SITE_ATTR)) {
          String siteNamePattern = sn.getAttributeValue(Office365Config.SITE_NAME_PATTERN_ATTR);
          List<Site> sites = session.getSites(siteNamePattern);
          for (Site site : sites) {
            // Setup root folder URI
            Logging.connectors.info("Seeding site " + site.displayName + " on domain " + config.getOrganizationDomain() + " (id: " + site.id + ").");
            if (!sitesToDelta.containsKey(site.id)) {
              // Seed the original delta request
              sitesToDelta.put(site.id, String.format("%s/sites/%s/drive/root/delta", session.getServiceRoot(), site.id));
            }
            fetchedSites.add(site.id);
          }
        }
      }

      // Process all sites that have a delta request
      List<ExecuteSeedingThread> seedingThreads = sitesToDelta.entrySet().stream()
        .map(s -> new ExecuteSeedingThread(s.getKey(), s.getValue())).collect(Collectors.toList());

      ExecutorService pool = Executors.newFixedThreadPool(8);
      for (final ExecuteSeedingThread seedThread : seedingThreads) {
        pool.execute(seedThread);
      }

      pool.shutdown();
      pool.awaitTermination(1, TimeUnit.MINUTES);

      for(ExecuteSeedingThread seedingThread : seedingThreads) {
        Exception e = seedingThread.getException();
        if (e != null) {
          if (isThrottle(e)) {
            // Keep the current deltaLink in the queue and revisit with next seeding
            Logging.connectors.warn("GraphAPI connection throttled.");
          } else {
            handleException(e);
          }
        } else {
          // Seed documents
          for (String documentIdentifier : seedingThread.getResult().documentIdentifiers) {
            activities.addSeedDocument(documentIdentifier);
          }
          // Update deltaLink
           sitesToDelta.put(seedingThread.getSiteId(), seedingThread.getResult().deltaLink);
        }
      }

      // TODO: If a site exists in the version string not in our query, then it's not available anymore so it all its documents should be entirely cleaned
    }
    catch(Exception e) {
      handleException(e);
    }

    // pack the sitesToDelta for next seeding
    String versionInfo = sitesToDelta.entrySet().stream().map(siteDelta -> siteDelta.getKey() + "::" + siteDelta.getValue()).collect(Collectors.joining(";;"));
    return versionInfo;
  }

  @Override
  public void processDocuments(String[] documentIdentifiers, IExistingVersions statuses, Specification spec,
                               IProcessActivity activities, int jobMode, boolean usesDefaultAuthority)
    throws ManifoldCFException, ServiceInterruption
  {
    for(String documentIdentifier : documentIdentifiers) {
      DriveItem driveItem = session.getDriveItem(documentIdentifier);

      // If driveItem is null and thus not found (404) delete from index.
      if (driveItem == null) {
        activities.deleteDocument(documentIdentifier);
        continue;
      }

      // There are only two allowed version states, either "null" which means it has been seeded by the deltaLink, or
      // "processed" which confirms the file has been processed by the routine below so no need to redo it.
      if (!activities.checkDocumentNeedsReindexing(documentIdentifier, "processed")) {
        continue;
      }

      // Don't do anything for folders as the deltaLink explicitly defines state of all the files from all folders.
      // If storing the /folder/file relationship becomes useful, use the activities.addDocumentReference and add the
      // logic to maintain relationship bearing in mind that deltaLink covers that logic already.
      if (driveItem.folder != null) {
        continue;
      }

      if (driveItem.size == 0L) {
        Logging.connectors.debug("Office365: Empty file not processed.");
        continue;
      }

      // We have a file that was seeded.
      // Flag the version as "processed". It will be reseeded by deltaLink api if any change is discovered.
      String version = "processed";

      if (Logging.connectors.isDebugEnabled()) {
        Logging.connectors.debug("Office365: Processing document identifier '" + documentIdentifier + "'");
        Logging.connectors.debug("Office365: have this file:\t" + driveItem.name);
      }

      long startTime = System.currentTimeMillis();
      String documentUri = null;
      String errorCode = null;
      String errorDesc = StringUtils.EMPTY;
      List<String> pathElem = new ArrayList<>();

      try {
        if (!activities.checkLengthIndexable(driveItem.size)) {
          errorCode = activities.EXCLUDED_LENGTH;
          errorDesc = "Excluding document because of file length ('"+driveItem.size+"')";
          activities.noDocument(documentIdentifier, version);
          continue;
        }

        if (!activities.checkMimeTypeIndexable(driveItem.file.mimeType))
        {
          errorCode = activities.EXCLUDED_MIMETYPE;
          errorDesc = "Excluding document because of mime type ("+driveItem.file.mimeType+")";
          activities.noDocument(documentIdentifier, version);
          continue;
        }

        if (!activities.checkDateIndexable(driveItem.lastModifiedDateTime.getTime()))
        {
          errorCode = activities.EXCLUDED_DATE;
          errorDesc = "Excluding document because of date ("+driveItem.lastModifiedDateTime.getTime()+")";
          activities.noDocument(documentIdentifier, version);
          continue;
        }

        RepositoryDocument rd = new RepositoryDocument();
        rd.setFileName(driveItem.name);
        rd.setCreatedDate(driveItem.fileSystemInfo.createdDateTime.getTime());
        rd.setModifiedDate(driveItem.fileSystemInfo.lastModifiedDateTime.getTime());
        rd.setIndexingDate(new Date());
        rd.setOriginalSize(driveItem.size);
        rd.setMimeType(driveItem.file.mimeType);

        // Harvest human readable paths to set in the rootPath (domain & site) and sourcePath (folder structure)
        Matcher mDomain = DOMAIN_PATTERN.matcher(driveItem.webUrl);
        Matcher mSite = SITE_PATTERN.matcher(driveItem.webUrl);
        if (mDomain.find()) pathElem.add(mDomain.group(1));
        if (mSite.find()) pathElem.add(mSite.group(1));

        rd.setRootPath(pathElem);

        String[] pathTokens = driveItem.parentReference.path.split(":");
        String folder;
        if (pathElem.size() > 0) folder = pathTokens[1];
        else folder = pathTokens[0];
        pathElem.addAll(Arrays.asList(StringUtils.strip(folder, "/").split("/")));

        rd.setSourcePath(pathElem);
        documentUri = pathElem.stream().map(p -> URLEncoder.encode(p)).collect(Collectors.joining("/", "/", "/")) + URLEncoder.encode(driveItem.name);

        // TODO, METADATA, ACL
        // driveItem.permissions.getCurrentPage() etc.

        // Fire up the document reading thread
        DocumentReadingThread t = new DocumentReadingThread(driveItem);
        try {
          t.start();
          boolean wasInterrupted = false;
          InputStream is = t.getSafeInputStream();
          try {
            rd.setBinary(is, driveItem.size);
            activities.ingestDocumentWithException(documentIdentifier, version, documentUri, rd);
          } catch (ManifoldCFException e) {
            if (e.getErrorCode() == ManifoldCFException.INTERRUPTED)
              wasInterrupted = true;
            throw e;
          } catch (Exception e) {
            handleException(e);
          } finally {
            is.close();
            if (!wasInterrupted)
              t.finishUp();
          }
          // No errors.  Record the fact that we made it.
          errorCode = "OK";
        } catch (InterruptedException e) {
          t.interrupt();
          throw new ManifoldCFException("Interrupted: " + e.getMessage(), e,
            ManifoldCFException.INTERRUPTED);
        } catch (Exception e) {
          handleException(e);
        }
      } catch (Exception e) {
        handleException(e);
      } finally {
        if (errorCode != null)
          activities.recordActivity(startTime, ACTIVITY_READ,
            driveItem.size, documentIdentifier, errorCode, errorDesc, null);
      }
    }
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
          List<Site> sites = session.getSites(siteNamePattern);
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

  protected static boolean isThrottle(Exception e) {
    if (e instanceof GraphServiceException) {
      int code = ((GraphServiceException)e).getResponseCode();
      return code == 503 || code == 509 || code == 429;
    } else if (e instanceof java.net.SocketTimeoutException || e instanceof InterruptedIOException) {
      return true;
    }
    return false;
  }

  protected static void handleException(Exception e)
    throws ManifoldCFException, ServiceInterruption
  {
    String errorMessage = String.format("Office365: %s with message: %s", e.getClass().getSimpleName(), e.getMessage());
    Logging.connectors.debug(errorMessage);
    e.printStackTrace();
    if (isThrottle(e)) {
      long currentTime = System.currentTimeMillis();
      throw new ServiceInterruption(errorMessage, e, currentTime + 300000L, currentTime + 3 * 60 * 60000L, -1, false);
    } else {
      throw new ManifoldCFException(errorMessage, e);
    }
  }

  protected class ExecuteSeedingThread extends Thread
  {
    protected final String siteId;

    protected final String siteDeltaUrl;

    protected Office365Session.DocumentDeltaResult documentDeltaResult= null;

    protected Exception exception = null;

    public ExecuteSeedingThread(String siteId, String siteDeltaUrl)
    {
      super();
      this.siteId = siteId;
      this.siteDeltaUrl = siteDeltaUrl;
      setDaemon(true);
    }

    @Override
    public void run()
    {
      try {
        documentDeltaResult = session.getDocumentIdentifiersFromDelta(this.siteDeltaUrl);
      } catch (Exception e) {
        exception = e;
      }
    }

    public Exception getException() { return exception; }

    public String getSiteId() { return siteId; }

    public Office365Session.DocumentDeltaResult getResult() { return documentDeltaResult; }
  }

  protected class DocumentReadingThread extends Thread {

    protected Throwable exception = null;
    protected final DriveItem driveItem;
    protected InputStream sourceStream;
    protected XThreadInputStream threadStream;
    protected boolean abortThread;

    public DocumentReadingThread(DriveItem driveItem) {
      super();
      this.driveItem = driveItem;
      setDaemon(true);
    }

    public void run()
    {
      try {
        try {
          synchronized (this) {
            if (!abortThread) {
              sourceStream = session.getDriveItemOutputStream(driveItem);
              threadStream = new XThreadInputStream(sourceStream);
              this.notifyAll();
            }
          }

          if (threadStream != null)
          {
            // Stuff the content until we are done
            threadStream.stuffQueue();
          }
        } finally {
          if (sourceStream != null) {
            sourceStream.close();
          }
        }
      } catch (Throwable e) {
        exception = e;
      }
    }

    public XThreadInputStream getSafeInputStream() throws InterruptedException
    {
      // Must wait until stream is created, or until we note an exception was thrown.
      while (true)
      {
        synchronized (this)
        {
          if (exception != null) {
            throw new IllegalStateException("Check for response before getting stream");
          }
          if (threadStream != null) {
            return threadStream;
          }
          wait();
        }
      }
    }

    public void finishUp() throws InterruptedException, IOException
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

    protected synchronized void checkException(Throwable exception) throws IOException
    {
      if (exception != null)
      {
        Throwable e = exception;
        if (e instanceof IOException) {
          throw (IOException)e;
        } else if (e instanceof RuntimeException) {
          throw (RuntimeException)e;
        } else if (e instanceof Error) {
          throw (Error)e;
        } else {
          throw new RuntimeException("Unhandled exception of type: "+e.getClass().getName(),e);
        }
      }
    }
  }
}
