package org.apache.manifoldcf.crawler.connectors.office365;

import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.logger.ILogger;
import com.microsoft.graph.logger.LoggerLevel;
import com.microsoft.graph.models.extensions.Drive;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.Site;
import com.microsoft.graph.options.QueryOption;
import com.microsoft.graph.requests.extensions.*;
import org.apache.manifoldcf.core.interfaces.Specification;
import org.apache.manifoldcf.core.interfaces.SpecificationNode;
import org.apache.manifoldcf.crawler.connectors.office365.functionalmanifold.XThreadObjectBuffer;
import org.apache.manifoldcf.crawler.system.Logging;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Office365Session
{
  private IGraphServiceClient graphClient;
  private Office365Config config;

  /*  There are the URL segments that are part of the 'system' document libraries that we are not interested in. There are no
      other flags that distinguish between a system library and a normal library when doing a ?select=id,name,webUrl,system to the
      drive query so the only way is to specify known system libraries we don't want (ie: all but Site Assets) and filter them out.
      We must use the webUrl as the Library Name can be changed by users, even for sytem ones.
  */
  private static final String[] SITE_INVALID_URL_SEGMENTS = {
          "/_catalogs",
          "/SitePages",
          "/Style%20Library",
          "/FormServerTemplates",
          "/IWConvertedForms"
  };

  public enum SitePatternField {
    DISPLAY_NAME,
    WEB_URL;
    public static SitePatternField fromString(String sitePatternFieldStr) {
      try
      {
        return SitePatternField.valueOf(sitePatternFieldStr);
      }
      catch (Exception e)
      {
        // If string is empty (parsed from a previous definition) or else undefined, revert to previous default
        // method of matching against the DisplayName of the site.
        return SitePatternField.DISPLAY_NAME;
      }
    }
  }

  public Office365Session(Office365Config config)
  {
    this.config = config;
    graphClient = GraphServiceClient
      .builder()
      .authenticationProvider(new Office365AuthenticationProvider(config))
      // the graph sdk logging sucks, just disable it
      .logger(new ILogger() {
        @Override
        public void setLoggingLevel(LoggerLevel loggerLevel) { /* Ignore */ }

        @Override
        public LoggerLevel getLoggingLevel() { return LoggerLevel.ERROR; }

        @Override
        public void logDebug(String s) {
          if(Logging.connectors.isDebugEnabled()) { Logging.connectors.debug("O365: " + s); }
        }

        @Override
        public void logError(String s, Throwable throwable) {
          Logging.connectors.error("O365: " + s);
        }
      })
      .buildClient();
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
        String sitePattern = sn.getAttributeValue(Office365Config.SITE_NAME_PATTERN_ATTR);
        String sitePatternFieldStr = sn.getAttributeValue(Office365Config.SITE_NAME_PATTERN_FIELD_ATTR);
        return getSites(sitePattern, SitePatternField.fromString(sitePatternFieldStr));
      }
    }
    throw new IllegalArgumentException("Specification did not contain site pattern.");
  }

  /**
   * Given a site id, return the Site.
   * @param id
   * @return site
   */
  public Site siteById(String id) {
    return graphClient.sites(id).buildRequest().get();
  }

  /**
   * Retrieve all the sites id that match the site name pattern.
   */
  public List<Site> getSites(String sitePattern, SitePatternField sitePatternField)
    throws ClientException
  {
    List<Site> sites = new ArrayList<>();

    String siteSearch;
    if (sitePattern.matches("[a-zA-Z0-9\\s]*")) siteSearch = String.format("{%s}", sitePattern);
    else siteSearch = "*";

    ISiteCollectionRequest request = graphClient.sites().buildRequest(Collections.singletonList(new QueryOption("search", siteSearch)));
    ISiteCollectionPage page = request.get();
    while (page != null) {
      sites.addAll(page.getCurrentPage().stream().filter(s -> {
        switch (sitePatternField)
        {
          case DISPLAY_NAME:
            // even though I don't see a way to make displayName null on the MS UI, there are some sites with displayName null, ignore these
            if (s.displayName == null) return false;
            if (siteSearch.equals("*")) return s.displayName.matches(sitePattern);
            else return s.displayName.equals(sitePattern); // site pattern of "A B" matches "A B C" so this makes it an exact match
          case WEB_URL:
            return s.webUrl.matches(sitePattern);
          default: throw new IllegalArgumentException("Unrecognized site pattern field");
        }
      }).collect(Collectors.toList()));
      page = page.getNextPage() == null ? null : page.getNextPage().buildRequest().get();
    }

    return sites;
  }

  public List<DriveItem> getDriveItems(String driveId) {
    List<DriveItem> items = new ArrayList<>();
    try {
      IDriveItemCollectionRequest request = graphClient.drives(driveId).root().children().buildRequest();
      IDriveItemCollectionPage page = request.get();
      while (page != null) {
        items.addAll(page.getCurrentPage());
        page = page.getNextPage() == null ? null : page.getNextPage().buildRequest().get();
      }
      return items;
    }
    catch (GraphServiceException e) {
      if (e.getResponseCode() == 404) {
        Logging.connectors.warn("O365: MSGraph Not Found (404) in getDriveItems, drive (" + driveId + ") \n" + e.getMessage());
        return items;
      } else if (e.getResponseCode() == 403) {
        Logging.connectors.warn("O365: MSGraph Permission Denied (403) in getDriveItems, drive (" + driveId + ") \n" + e.getMessage());
        return items;
      }
      else {
        throw e;
      }
    }
  }

  public void getDriveItemsUnderItem(String driveId, String itemId, XThreadObjectBuffer<DriveItem> b)
      throws InterruptedException
  {
    try {
      IDriveItemCollectionRequest request = graphClient.drives(driveId).items(itemId).children().buildRequest();
      IDriveItemCollectionPage page = request.get();
      while (page != null) {
        for (DriveItem driveItem : page.getCurrentPage()) {
          b.add(driveItem);
        }
        page = page.getNextPage() == null ? null : page.getNextPage().buildRequest().get();
      }
    }
    catch (GraphServiceException e) {
      if (e.getResponseCode() == 404) {
        Logging.connectors.warn("O365: MSGraph Not Found (404) in getDriveItemsUnderItem, drive (" + driveId + "), item (" + itemId + ")\n" + e.getMessage());
        return;
      } else if (e.getResponseCode() == 403) {
        Logging.connectors.warn("O365: MSGraph Permission Denied (403) in getDriveItemsUnderItem, drive (" + driveId + "), item (" + itemId + ")\n" + e.getMessage());
        return;
      } else {
        throw e;
      }
    }
  }

  public Drive driveById(String id) {
    return graphClient.drives(id).buildRequest().get();
  }

  public List<Drive> getDrivesForSite(String siteId)
          throws ClientException
  {
    try {
      List<Drive> drives = new ArrayList<>();
      /* We use the ?select API to provide the system selector which adds system libraries like "Site Assets"
          See: https://stackoverflow.com/questions/47562127/can-no-longer-find-sharepoint-site-assets-list-via-graph-api.
       */
      IDriveCollectionPage page = graphClient.sites(siteId).drives().buildRequest()
              .select("id,name,weburl,driveType,system")
              .get();

      while (page != null) {
        for (Drive drive : page.getCurrentPage()) {
          if (!drive.driveType.equals("documentLibrary")) {
            continue;
          }
          boolean validUrl = true;
          for (String invalidUrlPart : SITE_INVALID_URL_SEGMENTS) {
            if (drive.webUrl.contains(invalidUrlPart)) {
              validUrl = false;
              break;
            }
          }
          if (validUrl) {
            drives.add(drive);
          }
        }
        page = page.getNextPage() == null ? null : page.getNextPage().buildRequest().get();
      }
      return drives;
    } catch (GraphServiceException e) {
      if (e.getResponseCode() == 404 || e.getResponseCode() == 403) {
        return null;
      } else {
        throw e;
      }
    }
  }

  public Drive getDriveForSite(String siteId)
    throws ClientException
  {
    try {
      return graphClient.sites(siteId).drive().buildRequest().get();
    } catch (GraphServiceException e) {
      if (e.getResponseCode() == 404 || e.getResponseCode() == 403) {
        return null;
      } else {
        throw e;
      }
    }
  }

  public DriveItem getDriveItem(String driveId, String itemId)
    throws ClientException
  {
    try {
      return graphClient.drives(driveId).items(itemId).buildRequest().get();
    } catch (GraphServiceException e) {
      if (e.getResponseCode() == 404 || e.getResponseCode() == 403) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /** Get a stream representing the specified document.
   */
  public InputStream getDriveItemInputStream(DriveItem driveItem)
    throws ClientException
  {
    try {
      return graphClient
        .drives(driveItem.parentReference.driveId)
        .items(driveItem.id)
        .content()
        .buildRequest()
        .get();
    } catch (GraphServiceException e) {
      if (e.getResponseCode() == 404 || e.getResponseCode() == 403) {
        return null;
      } else {
        throw e;
      }
    }
  }

  public void close()
  {
    if (graphClient != null) {
      graphClient.shutdown();
      graphClient = null;
    }
  }
}
