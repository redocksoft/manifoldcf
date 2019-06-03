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

import com.microsoft.graph.http.GraphServiceException;
import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.crawler.system.Logging;

import java.io.InterruptedIOException;

public class Office365ErrorHandling {
  public static boolean isThrottle(Exception e) {
    if (e instanceof GraphServiceException) {
      int code = ((GraphServiceException)e).getResponseCode();
      return code == 503 || code == 509 || code == 429;
    } else return e instanceof InterruptedIOException;
  }

  public static void handleOtherException(Exception e) throws ManifoldCFException, ServiceInterruption {
    String errorMessage = String.format("O365: exception: %s with message: %s", e.getClass().getSimpleName(), e.getMessage());
    Logging.connectors.debug(errorMessage, e);
    if (isThrottle(e)) {
      long currentTime = System.currentTimeMillis();
      throw new ServiceInterruption(errorMessage, e, currentTime + 300000L, currentTime + 3 * 60 * 60000L, -1, false);
    }
  }
}
