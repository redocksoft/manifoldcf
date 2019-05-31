package org.apache.manifoldcf.agents.output.redock;

import org.apache.manifoldcf.core.interfaces.*;
import java.util.Locale;

public class ReDockConfig extends ReDockParam {

    final private static ParameterEnum[] CONFIGURATIONLIST =
    {
        ParameterEnum.SERVERLOCATION,
        ParameterEnum.CLIENTNAME,
        ParameterEnum.ENVIRONMENT,
        ParameterEnum.TOKEN
    };

    /** Build a set of reDockParameters by reading ConfigParams. If the
     * value returned by ConfigParams.getParameter is null, the default value is
     * set.
     *
     * @param params */
    public ReDockConfig(ConfigParams params)
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

    final public String getServerLocation()
    {
        return get(ParameterEnum.SERVERLOCATION);
    }

    final public String getClientName()
    {
        return get(ParameterEnum.CLIENTNAME);
    }

    final public String getEnvironment()
    {
        return get(ParameterEnum.ENVIRONMENT);
    }

    final public String getToken() { return get(ParameterEnum.TOKEN); }
}
