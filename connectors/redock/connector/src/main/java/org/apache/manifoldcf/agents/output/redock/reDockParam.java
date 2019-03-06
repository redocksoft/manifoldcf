package org.apache.manifoldcf.agents.output.redock;

import java.util.HashMap;
import java.util.Map;

import org.apache.manifoldcf.core.interfaces.IHTTPOutput;
import org.apache.manifoldcf.agents.output.redock.reDockParam.ParameterEnum;

/**
 * Parameters data for the reDock output connector.
 */
public class reDockParam extends HashMap<ParameterEnum, String> {
  /**
   * Parameters constants
   */
  public enum ParameterEnum {
    SERVERLOCATION("https://api.redock.com"),
    TOKEN("VALID_AUTHENTICATION_TOKEN_HERE"),
    CLIENTNAME(""),
    ENVIRONMENT("");

    final protected String defaultValue;

    ParameterEnum(String defaultValue) {
      this.defaultValue = defaultValue;
    }
  }

  protected reDockParam(ParameterEnum[] params) {
    super(params.length);
  }

  final public Map<String, Object> buildMap(IHTTPOutput out) {
    Map<String, Object> rval = new HashMap<>();
    for (Map.Entry<ParameterEnum, String> entry : this.entrySet()) {
      // See reDockParam as a template if we want to use
      final String key = entry.getKey().name();
      rval.put(key, entry.getValue());
    }
    return rval;
  }
}
