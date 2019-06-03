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
import org.apache.manifoldcf.connectorcommon.common.XThreadInputStream;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.crawler.system.Logging;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * In a background thread, runs a process that supplies arbitrary objects, as per the supplier in the constructor.
 * The caller thread then executes the code specified by {@link #onFetch} as they are obtained by the background
 * thread. All recommended ManifoldCF exception handling is taken care of.
 *
 * It would be nice if something like this (and similar ones for other common situations) were in core
 * Manifold, but we'll leave them here for now pending code review and acceptance by upstream.
 */
public class ThreadedObjectSupplier<T> extends BaseIoThread {
  private final ManifoldCheckedConsumer<XThreadObjectBuffer<T>> block;
  protected Throwable exception = null;
  protected final XThreadObjectBuffer<T> childBuffer;

  public ThreadedObjectSupplier(ManifoldCheckedConsumer<XThreadObjectBuffer<T>> block) {
    super();
    this.block = block;
    this.childBuffer = new XThreadObjectBuffer<T>();
    setDaemon(true);
  }

  public void run() {
    try {
      block.accept(childBuffer);
    } catch (Throwable e) {
      this.exception = e;
    } finally {
      childBuffer.signalDone();
    }
  }

  protected Throwable getException() {
    return exception;
  }

  protected void finishUp()
      throws InterruptedException, IOException {
    childBuffer.abandon();
    join();
    checkException(getException());
  }

  public void onFetch(ManifoldCheckedConsumer<T> onFetch) throws ManifoldCFException, ServiceInterruption {
    try {
      start();
      boolean wasInterrupted = false;
      try {
        while (true) {
          T child = childBuffer.fetch();
          if (child == null) {
            break;
          }
          onFetch.accept(child);
        }
      } catch (InterruptedException e) {
        wasInterrupted = true;
        throw e;
      } catch (ManifoldCFException e) {
        if (e.getErrorCode() == ManifoldCFException.INTERRUPTED)
          wasInterrupted = true;
        throw e;
      } finally {
        if (!wasInterrupted)
          finishUp();
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
  }
}
