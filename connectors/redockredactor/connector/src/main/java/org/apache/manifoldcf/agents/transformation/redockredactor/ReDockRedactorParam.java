/**
 * Copyright reDock Inc. 2020, All Rights Reserved
 */

package org.apache.manifoldcf.agents.transformation.redockredactor;

import java.util.HashMap;
import java.util.Map;

import org.apache.manifoldcf.core.interfaces.IHTTPOutput;
import org.apache.manifoldcf.agents.transformation.redockredactor.ReDockRedactorParam.ParameterEnum;

/**
 * Parameters data for the reDock output connector.
 */
public class ReDockRedactorParam extends HashMap<ParameterEnum, String> {
  /**
   * Parameters constants
   */
  public enum ParameterEnum {
    // Name of the tsv file to look for in the file-resources folder at the root of the manifold installation
    REPLACEMENTSPATH(""),
    CONNECTORNAME(""),
    REPLACEMENTSFOUND("0"),
    REPLACEMENTSEXCEPTION("");

    final protected String defaultValue;

    ParameterEnum(String defaultValue) {
      this.defaultValue = defaultValue;
    }
  }

  protected ReDockRedactorParam(ParameterEnum[] params) {
    super(params.length);
  }

  final public Map<String, Object> buildMap(IHTTPOutput out) {
    Map<String, Object> rval = new HashMap<>();
    for (Map.Entry<ParameterEnum, String> entry : this.entrySet()) {
      // See ReDockParam as a template if we want to use
      final String key = entry.getKey().name();
      rval.put(key, entry.getValue());
    }
    return rval;
  }
}
