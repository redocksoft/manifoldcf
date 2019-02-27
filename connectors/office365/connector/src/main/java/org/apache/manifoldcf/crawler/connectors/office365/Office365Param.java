package org.apache.manifoldcf.crawler.connectors.office365;

import java.util.HashMap;
import java.util.Map;
import org.apache.manifoldcf.core.interfaces.IHTTPOutput;
import org.apache.manifoldcf.crawler.connectors.office365.Office365Param.ParameterEnum;

/**
 * Parameters data for the reDock output connector.
 */
public class Office365Param extends HashMap<ParameterEnum, String> {
  /**
   * Parameters constants
   */
  public enum ParameterEnum {
    TENANT_ID(""),
    CLIENT_ID(""),
    CLIENT_SECRET(""),
    ORGANIZATION_DOMAIN(""),
    CONNECTION_TIMEOUT("60000"),
    SOCKET_TIMEOUT("1800000");

    final protected String defaultValue;

    ParameterEnum(String defaultValue) {
      this.defaultValue = defaultValue;
    }
  }

  protected Office365Param(ParameterEnum[] params) {
    super(params.length);
  }

  final public HashMap<String, Object> buildMap() {
    HashMap<String, Object> rval = new HashMap<>();
    for (Map.Entry<ParameterEnum, String> entry : this.entrySet()) {
      // See reDockParam as a template if we want to use
      final String key = entry.getKey().name();
      rval.put(key, entry.getValue());
    }
    return rval;
  }
}