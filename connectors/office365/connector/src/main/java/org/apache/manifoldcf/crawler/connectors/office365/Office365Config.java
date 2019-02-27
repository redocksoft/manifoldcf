package org.apache.manifoldcf.crawler.connectors.office365;

import org.apache.manifoldcf.core.interfaces.*;

import java.util.HashMap;
import java.util.Locale;

public class Office365Config extends Office365Param {

  public static final String SITE_ATTR = "site";
  public static final String SITE_NAME_PATTERN_ATTR = "namepattern";
  public static final String SITE_FOLDER_PATTERN_ATTR = "folderpattern";
  public static final String SITE_FILE_PATTERN_ATTR = "filepattern";
  public static final String SITE_STATUS_ATTR = "status";

  final private static ParameterEnum[] CONFIGURATIONLIST =
    {
      ParameterEnum.TENANT_ID,
      ParameterEnum.CLIENT_ID,
      ParameterEnum.CLIENT_SECRET,
      ParameterEnum.ORGANIZATION_DOMAIN,
      ParameterEnum.CONNECTION_TIMEOUT,
      ParameterEnum.SOCKET_TIMEOUT
    };

  /** Build a set of reDockParameters by reading ConfigParams. If the
   * value returned by ConfigParams.getParameter is null, the default value is
   * set.
   *
   * @param params */
  public Office365Config(ConfigParams params)
  {
    super(CONFIGURATIONLIST);
    for (ParameterEnum param : CONFIGURATIONLIST)
    {
      put(param, params.getParameter(param.name()), param.defaultValue);
    }
  }

  private void put(ParameterEnum param, String value, String defaultValue)
  {
    if (value == null) {
      put(param, defaultValue);
    } else {
      put(param, value);
    }
  }

  public final static String contextToConfig(IThreadContext threadContext,
                                             IPostParameters variableContext,
                                             ConfigParams parameters) throws ManifoldCFException
  {
    String rval = null;
    for (ParameterEnum param : CONFIGURATIONLIST)
    {
      final String paramName = param.name().toLowerCase(Locale.ROOT);
      String p = variableContext.getParameter(paramName);
      if (p != null)
      {
        parameters.setParameter(param.name(), p);
      }
    }

    return rval;
  }

  final public String getTenantId() { return get(ParameterEnum.TENANT_ID); }
  final public String getClientId() { return get(ParameterEnum.CLIENT_ID); }
  final public String getClientSecret() { return get(ParameterEnum.CLIENT_SECRET); }
  final public String getOrganizationDomain() { return get(ParameterEnum.ORGANIZATION_DOMAIN); }
}
