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
import org.apache.manifoldcf.crawler.connectors.office365.Office365Connector;
import org.apache.manifoldcf.crawler.system.Logging;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.function.Supplier;

/**
 * Handles the boiler-plate or running an I/O operation in a background thread, and returning a result,
 * with all the appropriate exception handling.
 *
 * It would be nice if something like this (and similar ones for other common situations) were in core
 * Manifold, but we'll leave them here for now pending code review and acceptance by upstream.
 */
public class ThreadedBlock<T> extends BaseIoThread {
  private final Supplier<T> block;
  protected T response = null;
  protected Throwable exception = null;

  public ThreadedBlock(Supplier<T> block) {
    super();
    this.block = block;
    setDaemon(true);
  }

  public void run() {
    try {
      this.response = block.get();
    } catch (Throwable e) {
      this.exception = e;
    }
  }

  public Throwable getException() {
    return exception;
  }

  public T runBlocking() throws ManifoldCFException, ServiceInterruption {
    try {
      start();
      join();
      Throwable thr = getException();
      if (thr != null) {
        if (thr instanceof IOException) {
          throw (IOException) thr;
        } else if (thr instanceof RuntimeException) {
          throw (RuntimeException) thr;
        } else {
          throw (Error) thr;
        }
      }
    } catch (java.net.SocketTimeoutException e) {
      Logging.connectors.warn("Socket timeout on I/O block: " + e.getMessage(), e);
      handleIOException(e);
    } catch (InterruptedException | InterruptedIOException e) {
      interrupt();
      throw new ManifoldCFException("Interrupted: " + e.getMessage(), e,
          ManifoldCFException.INTERRUPTED);
    } catch (IOException e) {
      Logging.connectors.warn("Error on I/O block: " + e.getMessage(), e);
      handleIOException(e);
    } catch (Exception e) {
      Logging.connectors.warn("Error on I/O block: " + e.getMessage(), e);
      handleOtherException(e);
    }
    return response;
  }
}
