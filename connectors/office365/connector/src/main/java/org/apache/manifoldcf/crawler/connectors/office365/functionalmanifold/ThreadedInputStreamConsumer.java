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
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.function.Supplier;

/**
 * In a background thread, runs a process that reads an input stream, as per the supplier in the constructor.
 * The caller thread then executes the code specified by {@link #onInput} as it is obtained by the background
 * thread. All recommended ManifoldCF exception handling is taken care of.
 *
 * It would be nice if something like this (and similar ones for other common situations) were in core
 * Manifold, but we'll leave them here for now pending code review and acceptance by upstream.
 */
public class ThreadedInputStreamConsumer extends BaseIoThread {
  protected final Supplier<InputStream> sourceStreamSupplier;
  protected InputStream sourceStream;
  protected XThreadInputStream threadStream;
  protected Throwable exception = null;

  protected boolean abortThread;

  /**
   * If the underlying API provides an input stream, call this constructor with the supplier for that stream.
   * Then call {@link #onInput}
   * @param sourceStreamSupplier
   */
  public ThreadedInputStreamConsumer(Supplier<InputStream> sourceStreamSupplier) {
    super();
    this.sourceStreamSupplier = sourceStreamSupplier;
    setDaemon(true);
  }

  public void run()
  {
    try {
      try {
        synchronized (this) {
          if (!abortThread) {
            sourceStream = sourceStreamSupplier == null ? null : sourceStreamSupplier.get();
            if (sourceStream == null) {
              throw new IllegalStateException("No content available.");
            }
            threadStream = new XThreadInputStream(sourceStream);
            this.notifyAll();
          }
        }

        if (threadStream != null)
        {
          // Stuff the content until we are done
          threadStream.stuffQueue();
        }
      } finally {
        if (sourceStream != null) {
          sourceStream.close();
        }
      }
    } catch (Throwable e) {
      exception = e;
    }
  }

  protected Throwable getException() {
    return exception;
  }

  protected XThreadInputStream getSafeInputStream() throws InterruptedException, IOException
  {
    // Must wait until stream is created in the background thread, or until we note an exception was thrown.
    while (true)
    {
      synchronized (this)
      {
        checkException(getException());
        if (threadStream != null) {
          return threadStream;
        }
        wait();
      }
    }
  }

  protected void finishUp() throws InterruptedException, IOException
  {
    // This will be called during the finally
    // block in the case where all is well (and
    // the stream completed) and in the case where
    // there were exceptions.
    synchronized (this) {
      if (threadStream != null) {
        threadStream.abort();
      }
      abortThread = true;
    }

    join();
    checkException(exception);
  }

  /**
   * Call if the underlying API provides an InputStream to read from.
   * @param onInput
   * @throws ManifoldCFException
   * @throws ServiceInterruption
   */
  public void onInput(ManifoldCheckedConsumer<InputStream> onInput) throws ManifoldCFException, ServiceInterruption {
    try {
      start();
      boolean wasInterrupted = false;
      try {
        try (InputStream is = getSafeInputStream()) {
          onInput.accept(is);
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
      Logging.connectors.warn("Socket timout on I/O block: " + e.getMessage(), e);
      handleIOException(e);
    } catch (InterruptedException | InterruptedIOException e) {
      // We were interrupted out of the join, most likely.  Before we abandon the thread,
      // send a courtesy interrupt.
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
