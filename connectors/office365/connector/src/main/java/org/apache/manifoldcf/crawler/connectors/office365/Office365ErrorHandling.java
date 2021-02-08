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

import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphFatalServiceException;
import com.microsoft.graph.http.GraphServiceException;
import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.crawler.system.Logging;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutionException;

public class Office365ErrorHandling {
  public static boolean isThrottle(Exception e) {
    if (e instanceof GraphServiceException) {
      int code = ((GraphServiceException)e).getResponseCode();
      // we include 401 here, as it appears that sometimes the 401 is thrown as a rate limit, but lets try and get more information
      if(code == 401) {
        String errorMessage = String.format("Unexpected 401 error, message=%s", ((GraphServiceException)e).getMessage(true));
        Logging.connectors.warn(errorMessage, e);
      }
      return code == 503 || code == 509 || code == 429 || code == 401;
    } else return e instanceof InterruptedIOException;
  }

  public static boolean isRetryableConnectionFailure(Exception e) {
    if (e instanceof GraphFatalServiceException) {
      // sometimes the graph API throws a 500 Internal Server Error for no particular reason, consider it a retryable error
      return true;
    }

    // graph API often throws various connection resets or broken pipe socket exceptions, these IOException's are retryable errors
    Throwable cause = e;
    if (e instanceof ClientException) {
      cause = e.getCause();
    }
    return cause instanceof IOException;
  }

  public static void handleExecutionException(ExecutionException e) throws ManifoldCFException, ServiceInterruption {
    if(e.getCause() instanceof ManifoldCFException) {
      throw (ManifoldCFException)e.getCause();
    } else if(e.getCause() instanceof ServiceInterruption) {
      throw (ServiceInterruption)e.getCause();
    } else {
      handleGraphExceptions(e);
      handleOtherException(e);
    }
  }

  public static void handleOtherException(Exception e) throws ManifoldCFException, ServiceInterruption {
    if (e instanceof ManifoldCFException) {
      throw ((ManifoldCFException) e);
    } else if (e instanceof ServiceInterruption) {
      throw ((ServiceInterruption) e);
    } else {
      throw new ManifoldCFException(e);
    }
  }

  public static void handleGraphExceptions(Exception e) throws ManifoldCFException, ServiceInterruption {
    if (isThrottle(e)) {
      String errorMessage = String.format("O365: exception resulting in throttle: %s with message: %s", e.getClass().getSimpleName(), e.getMessage());
      Logging.connectors.warn(errorMessage, e);
      long currentTime = System.currentTimeMillis();
      throw new ServiceInterruption(errorMessage, e, currentTime + 300000L, currentTime + 3 * 60 * 60000L, -1, false);
    } else if(isRetryableConnectionFailure(e)) {
      String errorMessage = String.format("O365: exception resulting in retryable connection failure: %s with message: %s", e.getClass().getSimpleName(), e.getMessage());
      Logging.connectors.warn(errorMessage, e);
      long currentTime = System.currentTimeMillis();
      throw new ServiceInterruption(errorMessage, e, currentTime + 1000L, -1L, 5, true);
    }
  }
}
