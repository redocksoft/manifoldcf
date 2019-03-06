package org.apache.manifoldcf.crawler.connectors.office365;

import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.Drive;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.Site;
import com.microsoft.graph.requests.extensions.*;
import org.apache.manifoldcf.core.util.URLEncoder;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Office365Session
{
  private static String APPNAME = "ManifoldCF Office365 Connector";

  private static final Pattern DRIVE_PATTERN = Pattern.compile("drives/(.*?)(/|$|\\?)");

  private IGraphServiceClient graphClient;
  private Office365Config config;

  public Office365Session(Office365Config config)
  {
    this.config = config;

    if (graphClient == null) {
      graphClient = GraphServiceClient
        .builder()
        .authenticationProvider(new Office365AuthenticationProvider(config))
        .buildClient();
    }
  }

  public String  getServiceRoot() { return graphClient.getServiceRoot(); }

  public String check()
  {
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

  /**
   * Retrieve all the sites id that match the site name pattern
   */
  public List<Site> getSites(String siteNamePattern)
    throws ClientException
  {
    List<Site> sites = new ArrayList();

    String siteSearch;
    if (siteNamePattern.matches("[a-zA-Z0-9\\s]*")) siteSearch = siteNamePattern;
    else siteSearch = "*";

    String siteEnumerationQuery = String.format("%s/%s/sites?search=%s",
      graphClient.getServiceRoot(), config.getOrganizationDomain(), URLEncoder.encode(siteSearch));

    ISiteCollectionRequestBuilder reqBuilder = new SiteCollectionRequestBuilder(siteEnumerationQuery, graphClient, null);

    while (reqBuilder != null) {
      ISiteCollectionPage siteCollectionPage = reqBuilder.buildRequest().get();
      sites.addAll(siteCollectionPage.getCurrentPage());
      reqBuilder = siteCollectionPage.getNextPage();
    }

    // Note that the search is made against display name so the patterns also should be consistent
    sites.removeIf(s -> {
      if (siteSearch.equals("*")) return !s.displayName.matches(siteNamePattern);
      else return !s.displayName.equals(siteSearch);
    });

    return sites;
  }

  public class DriveDeltaResult
  {
    public String newDeltaLink;
    public String driveId;
    public List<String> documentIdentifiers = new ArrayList<>();
  }

  /**
   * Returns the collection of DriveItem representing files for a at a specific newDeltaLink.
   * This also returns the next newDeltaLink.  Folders are omitted as the delta collection explicitly describes the state of each individual file.
   * @param deltaLink
   * @return
   * @throws ClientException
   */
  public DriveDeltaResult getDocumentIdentifiersFromDelta(String deltaLink)
  throws ClientException
  {
    DriveDeltaResult result = new DriveDeltaResult();
    IDriveItemDeltaCollectionRequestBuilder reqBuilder = new DriveItemDeltaCollectionRequestBuilder(deltaLink, graphClient, null);

    while (reqBuilder != null) {
      IDriveItemDeltaCollectionPage driveItemDeltaCollectionPage = reqBuilder.buildRequest().get();
      for (DriveItem driveItem : driveItemDeltaCollectionPage.getCurrentPage()) {
        if (driveItem.folder == null) {
          String documentIdentifier = String.format("drives/%s/items/%s", driveItem.parentReference.driveId, driveItem.id);
          // Here, we get files that were created AND deleted (driveItem.deleted).  The processDocuments routine will delete documents upon getting a 404
          // from the graph client.  Since we have the delete information, we could think about passing it into the processing.  However,
          // altering the documentIdentifier with a ?op=delete for instance creates a new document and doesn't load the correct one into context.
          // Would probably need to persist the list of delete request using storage.  To limit development time, we will take the hit of doing a request on the
          // file and do a 404.
          result.documentIdentifiers.add(documentIdentifier);
          if (result.driveId == null) result.driveId = driveItem.parentReference.driveId;
        }
      }
      if (result.newDeltaLink == null) {
        result.newDeltaLink = driveItemDeltaCollectionPage.deltaLink();
      }
      reqBuilder = driveItemDeltaCollectionPage.getNextPage();
    }

    return result;
  }

  public Drive getDriveForSite(String siteId)
    throws ClientException
  {
    Drive drive;
    IDriveRequestBuilder reqBuilder = new DriveRequestBuilder(String.format("%s/sites/%s/drive", graphClient.getServiceRoot(), siteId), graphClient, null);
    try {
      drive = reqBuilder.buildRequest().get();
    } catch (GraphServiceException e) {
      if (e.getResponseCode() == 404 || e.getResponseCode() == 403) {
        return null;
      } else {
        throw e;
      }
    }
    return drive;
  }

  public String getDriveIdFromUrlSegment(String deltaLink) {
    Matcher driveMatcher = DRIVE_PATTERN.matcher(deltaLink);
    if (driveMatcher.find()) {
      return driveMatcher.group(1);
    }
    return null;
  }

  public Drive getDrive(String documentIdentifier)
    throws ClientException
  {
    Drive drive;
    IDriveRequestBuilder reqBuilder = new DriveRequestBuilder(String.format("%s/%s", graphClient.getServiceRoot(), documentIdentifier), graphClient, null);
    try {
      drive = reqBuilder.buildRequest().get();
    } catch (GraphServiceException e) {
      if (e.getResponseCode() == 404) {
        return null;
      } else {
        throw e;
      }
    }
    return drive;
  }

  public DriveItem getDriveItem(String documentIdentifier)
    throws ClientException
  {
    DriveItem driveItem;
    IDriveItemRequestBuilder reqBuilder = new DriveItemRequestBuilder(String.format("%s/%s", graphClient.getServiceRoot(), documentIdentifier), graphClient, null);
    try {
      driveItem = reqBuilder.buildRequest().get();
    } catch (GraphServiceException e) {
      if (e.getResponseCode() == 404) {
        return null;
      } else {
        throw e;
      }
    }
    return driveItem;
  }

  /** Get a stream representing the specified document.
   */
  public InputStream getDriveItemOutputStream(DriveItem driveItem)
    throws ClientException
  {
     return graphClient
       .drives(driveItem.parentReference.driveId)
       .items(driveItem.id)
       .content()
       .buildRequest()
       .get();
  }

  public void close()
  {
    if (graphClient != null) {
      graphClient.shutdown();
      graphClient = null;
    }
  }
}
