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

package org.apache.manifoldcf.crawler.connectors.office365.functionalmanifold;

import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.crawler.connectors.office365.Office365ErrorHandling;

import java.io.IOException;
import java.io.InterruptedIOException;

abstract class BaseIoThread extends Thread {
  protected synchronized void checkException(Throwable exception) throws IOException {
    if(exception == null) return;

    if (exception instanceof IOException) {
      throw (IOException)exception;
    } else if (exception instanceof RuntimeException) {
      throw (RuntimeException)exception;
    } else if (exception instanceof Error) {
      throw (Error)exception;
    } else {
      throw new RuntimeException("Unhandled exception of type: "+exception.getClass().getName(),exception);
    }
  }

  protected void handleIOException(IOException e)
    throws ManifoldCFException, ServiceInterruption
  {
    if (!(e instanceof java.net.SocketTimeoutException) && (e instanceof InterruptedIOException)) {
      throw new ManifoldCFException("Interrupted: " + e.getMessage(), e,
        ManifoldCFException.INTERRUPTED);
    }
    long currentTime = System.currentTimeMillis();
    throw new ServiceInterruption("IO exception: "+e.getMessage(), e, currentTime + 300000L,
      currentTime + 3 * 60 * 60000L,-1,false);
  }

  protected void handleOtherException(Exception e)
    throws ManifoldCFException, ServiceInterruption
  {
    Office365ErrorHandling.handleOtherException(e);
  }
}
