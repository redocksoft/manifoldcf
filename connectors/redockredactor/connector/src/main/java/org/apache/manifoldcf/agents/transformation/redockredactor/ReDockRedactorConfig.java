/**
 * Copyright reDock Inc. 2020, All Rights Reserved
 */

package org.apache.manifoldcf.agents.transformation.redockredactor;

import org.apache.manifoldcf.core.interfaces.ConfigParams;
import org.apache.manifoldcf.core.interfaces.IPostParameters;

import java.util.Locale;

public class ReDockRedactorConfig extends ReDockRedactorParam {

    final private static ParameterEnum[] CONFIGURATIONLIST =
            {
                    ParameterEnum.REPLACEMENTSPATH,
                    ParameterEnum.CONNECTORNAME,
                    ParameterEnum.REPLACEMENTSFOUND // readonly
            };

    /**
     * Build a set of reDockParameters by reading ConfigParams. If the
     * value returned by ConfigParams.getParameter is null, the default value is
     * set.
     *
     * @param params
     */
    public ReDockRedactorConfig(ConfigParams params) {
        super(CONFIGURATIONLIST);
        for (ParameterEnum param : CONFIGURATIONLIST) {
            put(param, params.getParameter(param.name()), param.defaultValue);
        }
    }

    public final static String contextToConfig(IPostParameters variableContext, ConfigParams parameters) {
        for (ParameterEnum param : CONFIGURATIONLIST) {
            final String paramName = param.name().toLowerCase(Locale.ROOT);
            String p = variableContext.getParameter(paramName);
            if (p != null) {
                parameters.setParameter(param.name(), p);
            }
        }

        return null;
    }

    final public String getReplacementsPath() {
        return get(ParameterEnum.REPLACEMENTSPATH);
    }

    // TODO there might be a better/easier way to do this but I could not find one yet
    final public String getConnectorName() {
        return get(ParameterEnum.CONNECTORNAME);
    }

    private void put(ParameterEnum param, String value, String defaultValue) {
        if (value == null) {
            put(param, defaultValue);
        } else {
            put(param, value);
        }
    }
}
