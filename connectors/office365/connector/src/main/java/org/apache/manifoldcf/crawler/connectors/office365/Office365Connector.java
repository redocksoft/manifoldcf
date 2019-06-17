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

import com.microsoft.graph.models.extensions.Drive;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.Site;
import org.apache.commons.lang.StringUtils;
import org.apache.manifoldcf.agents.interfaces.IOutputHistoryActivity;
import org.apache.manifoldcf.agents.interfaces.RepositoryDocument;
import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector;
import org.apache.manifoldcf.crawler.connectors.office365.functionalmanifold.ThreadedInputStreamConsumer;
import org.apache.manifoldcf.crawler.connectors.office365.functionalmanifold.ThreadedObjectSupplier;
import org.apache.manifoldcf.crawler.interfaces.IExistingVersions;
import org.apache.manifoldcf.crawler.interfaces.IProcessActivity;
import org.apache.manifoldcf.crawler.interfaces.ISeedingActivity;
import org.apache.manifoldcf.crawler.system.Logging;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This repository connector takes the configuration of Microsoft Application and reads the SharePoint Online sites that match
 * a site name pattern.  The application needs File.Read.All and Site.Read.ALl at the Microsoft Graph Level, and Sites.Read.All
 * at the SharePoint level.
 *
 * The connector does a full crawl of the source repository on each job run. The seeds are each site that matches the
 * site selector configuration. In the future, we should be able to make the crawl more network and CPU efficient by
 * taking advantage of the delta api
 * https://docs.microsoft.com/en-us/onedrive/developer/rest-api/concepts/scan-guidance?view=odsp-graph-online to read
 * only the new, changed, or deleted files. The main problem with using the delta API is that it operates at the site
 * level, and not at the level above. Since our seeds are sites (and this is useful for carry-down data from site to
 * document, as well as to track the relationship between document and site explicitly in Manifold), the delta API
 * has to be called in {@link #processDocuments}, but cannot be because Manifold expects the added/changed/deleted
 * documents to be provided by {@link #addSeedDocuments}. Need to think about how to work around this.
 */
public class Office365Connector extends BaseRepositoryConnector
{
  protected Office365Session session;

  private final static String ACTIVITY_READ = "read document";
  private final static String ACTIVITY_DELETE_DOCUMENT = "delete document";

  private final static String ACTIVITY_FETCH = "fetch";

  private static final String RELATIONSHIP_CONTAINED = "contained";
  private static final String RELATIONSHIP_CHILD = "child";

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
  public int getConnectorModel()
  {
    return MODEL_ALL;
  }

  @Override
  public String[] getActivitiesList() {
    return new String[]{ACTIVITY_FETCH, ACTIVITY_READ};
  }

  @Override
  public String[] getRelationshipTypes()
  {
    // contained is a document in a site, child is a document/folder in a folder
    return new String[] {RELATIONSHIP_CONTAINED, RELATIONSHIP_CHILD};
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

  final private Office365Config getConfigParameters()
  {
    return getConfigParameters(null);
  }

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
   * drive API (https://docs.microsoft.com/en-us/onedrive/developer/rest-api/concepts/scan-guidance?view=odsp-graph-online).
   *
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
    Logging.connectors.info("O365: seeding sites to be traversed");

    Office365Config config = getConfigParameters();

    for (Site site : new Office365ThreadedBlock<>(() -> session.currentSites(spec)).runBlocking()) {
      Drive drive = new Office365ThreadedBlock<>(() -> session.getDriveForSite(site.id)).runBlocking();

      // we create one seed "virtual" document for each site that needs to be traversed
      // the format is SITESEED::siteid::driveid
      Logging.connectors.info(String.format("O365: Seeding site %s on domain %s (id: %s, driveid: %s)", site.displayName, config.getOrganizationDomain(), site.id, drive.id));
      activities.addSeedDocument(String.format("SITESEED::%s::%s", site.id, drive.id));
    }

    return null;
  }

  @Override
  public void processDocuments(String[] documentIdentifiers, IExistingVersions statuses, Specification spec,
                               IProcessActivity activities, int jobMode, boolean usesDefaultAuthority)
    throws ManifoldCFException, ServiceInterruption
  {
    Logging.connectors.info(String.format("O365: Processing documents: %s", Arrays.deepToString(documentIdentifiers)));

    for (String documentIdentifier : documentIdentifiers) {
      String[] idComponents = documentIdentifier.split("::");
      String siteId = idComponents[1];
      String driveId = idComponents[2];

      if (documentIdentifier.startsWith("SITESEED::")) {
        // TODO do in a background thread, do like GetChildrenThread in Google, and XThreadStringBuffer to process
        // the items as they are read
        for (DriveItem item : new Office365ThreadedBlock<>(() -> session.getDriveItems(driveId)).runBlocking()) {
          String childDocId = String.format("DOC::%s::%s::%s", siteId, driveId, item.id);
          activities.addDocumentReference(childDocId, documentIdentifier, RELATIONSHIP_CONTAINED,
              new String[]{"Site"}, new String[][]{new String[]{siteId}});
        }
      } else if (documentIdentifier.startsWith("DOC::")) {
        String itemId = idComponents[3];

        DriveItem item = new Office365ThreadedBlock<>(() -> session.getDriveItem(driveId, itemId)).runBlocking();

        if(item.folder != null) {
          // folder
          if (Logging.connectors.isDebugEnabled()) {
            Logging.connectors.debug(String.format("O365: item %s is a folder", item.id));
          }

          // adding all the children + subdirs for a folder
          ThreadedObjectSupplier<DriveItem> driveItemSupplier = new Office365ThreadedObjectSupplier<>(b ->
              session.getDriveItemsUnderItem(driveId, item.id, b)
          );
          driveItemSupplier.onFetch(childItem -> {
            // TODO add other carry-down data from the site, and the parent folder?
            String childDocId = String.format("DOC::%s::%s::%s", siteId, driveId, childItem.id);
            activities.addDocumentReference(childDocId, documentIdentifier, RELATIONSHIP_CHILD,
                new String[] {"Site"}, new String[][]{new String[] {siteId}});
          });
        } else {
          // file
          if (Logging.connectors.isDebugEnabled()) {
            Logging.connectors.debug(String.format("O365: item %s is a file", item.id));
          }

          processDriveDocument(documentIdentifier, siteId, driveId, item, activities);
        }
      } else {
        throw new IllegalStateException("Unexpected document identifier " + documentIdentifier);
      }
    }
  }

  private void processDriveDocument(String documentIdentifier, String siteId, String driveId, DriveItem driveItem,
                                    IProcessActivity activities)
    throws ManifoldCFException, ServiceInterruption
  {
    long startTime = System.currentTimeMillis();
    String activity = ACTIVITY_READ;
    String documentUri = null;
    String resultCode = null;
    String resultDesc = StringUtils.EMPTY;
    Long fileSize = 0L;

    try {
      // If driveItem is null and thus not found (404) delete from index.
      if (driveItem == null) {
        activities.deleteDocument(documentIdentifier);
        activity = ACTIVITY_DELETE_DOCUMENT;
        resultCode = "OK";
        return;
      }

      // We have a file that was seeded.
      if (Logging.connectors.isDebugEnabled()) {
        Logging.connectors.debug("O365: Processing document identifier '" + documentIdentifier + "' with name '" + driveItem.name + "'");
      }

      // Version is the etag (any change in file content or metadata changes the version)
      // we also have driveItem.publication.versionId, perhaps that might be of some use?
      String version = driveItem.eTag;
      if (driveItem.size != null) fileSize = driveItem.size;

      if (!activities.checkDocumentNeedsReindexing(documentIdentifier, version)) {
        return;
      }

      // We shouldn't get a folder here
      if (driveItem.folder != null) {
        throw new IllegalStateException("Did not expect a folder item.");
      }

      if (fileSize == 0L) {
        resultCode = IOutputHistoryActivity.EXCEPTION;
        resultDesc = "Empty file";
        Logging.connectors.debug("O365: Empty file not processed.");
        return;
      }

      // URI tokens
      List<String> pathElem = new ArrayList<>();

      if (!activities.checkLengthIndexable(driveItem.size)) {
        resultCode = activities.EXCLUDED_LENGTH;
        resultDesc = "Excluding document because of file length ('" + driveItem.size + "')";
        activities.noDocument(documentIdentifier, version);
        return;
      }

      if (!activities.checkMimeTypeIndexable(driveItem.file.mimeType)) {
        resultCode = activities.EXCLUDED_MIMETYPE;
        resultDesc = "Excluding document because of mime type (" + driveItem.file.mimeType + ")";
        activities.noDocument(documentIdentifier, version);
        return;
      }

      if (!activities.checkDateIndexable(driveItem.lastModifiedDateTime.getTime())) {
        resultCode = activities.EXCLUDED_DATE;
        resultDesc = "Excluding document because of date (" + driveItem.lastModifiedDateTime.getTime() + ")";
        activities.noDocument(documentIdentifier, version);
        return;
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
      if (pathTokens.length > 1) {
        pathElem.addAll(Arrays.asList(StringUtils.strip(pathTokens[1], "/").split("/")));
      }

      rd.setSourcePath(pathElem);

      try {
        // other connectors seem to use URI as a URL, and the documentations states the URI is "displayed in search engine
        // "results as the link to the document", which is odd as URIs are not necessarily valid links/URLs
        // here we'll provide an actual URI, i.e. the key information necessary to access the item via the MS Graph
        // API: site, drive, and item ids, and we'll set the source URL as a separate property
        documentUri = new URI(
            "office365",
            getConfigParameters().getOrganizationDomain(),
            "/" + siteId + "/" + driveId + "/" + driveItem.id,
            null
        ).toString();
      } catch (URISyntaxException e) {
        throw new ManifoldCFException("Bad uri: "+e.getMessage(),e);
      }

      // Set other source fields
      rd.addField("source", "Microsoft Office 365");
      // the source URL is the web-accessible URL provided by the API -- note that driveItem.webUrl can and does
      // change, e.g. when a document is updated (though its seems to forward to the same place), so this value cannot
      // be used as the Manifold document URI
      rd.addField("sourceUrl", driveItem.webUrl);
      rd.addField("office365.org", getConfigParameters().getOrganizationDomain());
      rd.addField("office365.siteId", siteId);
      rd.addField("office365.driveId", driveId);
      rd.addField("office365.id", driveItem.id);

      // TODO, METADATA, ACL
      // driveItem.permissions.getCurrentPage() etc.

      // Read data in a background thread
      ThreadedInputStreamConsumer streamConsumer = new Office365ThreadedInputStreamConsumer(() -> session.getDriveItemInputStream(driveItem));
      String finalDocumentUri = documentUri;
      streamConsumer.onInput(is -> {
        rd.setBinary(is, driveItem.size);
        activities.ingestDocumentWithException(documentIdentifier, version, finalDocumentUri, rd);
      });
      resultCode = "OK";
    } catch (ManifoldCFException e) {
      if (e.getErrorCode() == ManifoldCFException.INTERRUPTED)
        resultCode = null;
      throw e;
    } finally {
      if (resultCode != null)
        activities.recordActivity(startTime, activity,
                fileSize, documentUri, resultCode, resultDesc, null);
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
          Logging.connectors.debug("O365: getSites exception: " + e.getMessage());
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
}
