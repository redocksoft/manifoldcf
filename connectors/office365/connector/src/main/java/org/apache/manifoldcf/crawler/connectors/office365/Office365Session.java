package org.apache.manifoldcf.crawler.connectors.office365;

import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.Drive;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.Site;
import com.microsoft.graph.requests.extensions.*;
import org.apache.manifoldcf.core.interfaces.Specification;
import org.apache.manifoldcf.core.interfaces.SpecificationNode;
import org.apache.manifoldcf.core.util.URLEncoder;
import org.apache.manifoldcf.crawler.connectors.office365.functionalmanifold.XThreadObjectBuffer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class Office365Session
{
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
        resultMsg.append("Could not connect to domain \"" + config.getOrganizationDomain() + "\".");
      }
    }
    catch(Exception ex) {
      resultMsg.append("Could not connect to domain \"" + config.getOrganizationDomain() + "\". Exception: " + ex.getMessage());
    }

    return resultMsg.toString();
  }

  /**
   * Given a job specification, return the current Office 365 sites that match.
   * @param spec
   * @return sites
   */
  public List<Site> currentSites(Specification spec) {
    for (int i = 0; i < spec.getChildCount(); i++) {
      SpecificationNode sn = spec.getChild(i);
      if (sn.getType().equals(Office365Config.SITE_ATTR)) {
        String siteNamePattern = sn.getAttributeValue(Office365Config.SITE_NAME_PATTERN_ATTR);
        return getSites(siteNamePattern);
      }
    }
    throw new IllegalArgumentException("Specification did not contain site pattern.");
  }

  /**
   * Retrieve all the sites id that match the site name pattern.
   */
  public List<Site> getSites(String siteNamePattern)
    throws ClientException
  {
    List<Site> sites = new ArrayList<>();

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

  public List<DriveItem> getDriveItems(String driveId) {
    List<DriveItem> items = new ArrayList<>();
    IDriveItemCollectionRequest request = graphClient.drives(driveId).root().children().buildRequest();
    IDriveItemCollectionPage page = request.get();
    while (page != null) {
      items.addAll(page.getCurrentPage());
      page = page.getNextPage() == null ? null : page.getNextPage().buildRequest().get();
    }
    return items;
  }

  public void getDriveItemsUnderItem(String driveId, String itemId, XThreadObjectBuffer<DriveItem> b)
      throws InterruptedException
  {
    IDriveItemCollectionRequest request = graphClient.drives(driveId).items(itemId).children().buildRequest();
    IDriveItemCollectionPage page = request.get();
    while (page != null) {
      for (DriveItem driveItem : page.getCurrentPage()) {
        b.add(driveItem);
      }
      page = page.getNextPage() == null ? null : page.getNextPage().buildRequest().get();
    }
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

  public DriveItem getDriveItem(String driveId, String itemId)
    throws ClientException
  {
    return graphClient.drives(driveId).items(itemId).buildRequest().get();
  }

  /** Get a stream representing the specified document.
   */
  public InputStream getDriveItemInputStream(DriveItem driveItem)
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
