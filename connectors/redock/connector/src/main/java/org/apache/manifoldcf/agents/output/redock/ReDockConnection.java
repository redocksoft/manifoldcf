/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.manifoldcf.agents.output.redock;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.manifoldcf.agents.interfaces.IOutputHistoryActivity;
import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class ReDockConnection {

  public enum Result {
    OK, NOT_FOUND_ON_ENDPOINT, ERROR, UNKNOWN
  }

  protected final ReDockConfig config;

  private final HttpClient client;
  private final String serverLocation;

  private String resultDescription = "";
  private String callUrlSnippet = null;
  private String response = null;
  private String resultCode = null;
  private Result result = Result.UNKNOWN;

  protected final static String jsonException = "\"error\"";
  protected final ObjectMapper objectMapper;

  protected ReDockConnection(ReDockConfig config, HttpClient client) {
    this.config = config;
    this.client = client;
    this.serverLocation = config.getServerLocation();
    this.objectMapper = new ObjectMapper();
  }

  protected StringBuffer getApiUrl() {
    StringBuffer url = new StringBuffer(serverLocation);
    url.append("/rest/v1/documents/");
    callUrlSnippet = url.toString();
    return url;
  }

  protected static class CallThread extends Thread {
    protected final HttpClient client;
    protected final HttpRequestBase method;
    protected int resultCode = -1;
    protected String response = null;
    protected Throwable exception = null;

    public CallThread(HttpClient client, HttpRequestBase method) {
      this.client = client;
      this.method = method;
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        try {
          HttpResponse resp = client.execute(method);
          resultCode = resp.getStatusLine().getStatusCode();
          response = getResponseBodyAsString(resp.getEntity());
        } finally {
          method.abort();
        }
      } catch (java.net.SocketTimeoutException e) {
        exception = e;
      } catch (InterruptedIOException e) {
        // Just exit
      } catch (Throwable e) {
        exception = e;
      }
    }

    public void finishUp()
      throws HttpException, IOException, InterruptedException {
      join();
      Throwable t = exception;
      if (t != null) {
        if (t instanceof HttpException)
          throw (HttpException) t;
        else if (t instanceof IOException)
          throw (IOException) t;
        else if (t instanceof RuntimeException)
          throw (RuntimeException) t;
        else if (t instanceof Error)
          throw (Error) t;
        else
          throw new RuntimeException("Unexpected exception thrown: " + t.getMessage(), t);
      }
    }

    public int getResultCode() {
      return resultCode;
    }

    public String getResponse() {
      return response;
    }

    public Throwable getException() {
      return exception;
    }
  }

  /** Call ElasticSearch.
   *@return false if there was a "rejection".
   */
  protected boolean call(HttpRequestBase method)
    throws ManifoldCFException, ServiceInterruption {
    CallThread ct = new CallThread(client, method);
    try {
      ct.start();
      try {
        ct.finishUp();
        response = ct.getResponse();
        return handleResultCode(ct.getResultCode(), response, method.getMethod());
      } catch (InterruptedException e) {
        ct.interrupt();
        throw new ManifoldCFException("Interrupted: " + e.getMessage(), e, ManifoldCFException.INTERRUPTED);
      }
    } catch (HttpException e) {
      handleHttpException(e);
      return false;
    } catch (IOException e) {
      handleIOException(e);
      return false;
    }
  }

  protected boolean handleResultCode(int code, String response, String method)
    throws ManifoldCFException, ServiceInterruption {
    if (code == 200 || code == 201) {
      setResult("OK", Result.OK, null);
      return true;
    } else if (code == 404 && "DELETE".equals(method)) {
      // The file was not found on the endpoint, perhaps because it was deleted by another mean.
      // Do not halt the job altogether, simply flag the file for audit purposes but carry-on job.
      setResult("FILE_NOT_FOUND", Result.NOT_FOUND_ON_ENDPOINT, "File does not exist on the endpoint.");
      return true;
    } else if (code == 404) {
      setResult(IOutputHistoryActivity.HTTP_ERROR, Result.ERROR, "Page not found: " + response);
      throw new ManifoldCFException("Server/page not found");
    } else if (code >= 400 && code < 500) {
      setResult(IOutputHistoryActivity.HTTP_ERROR, Result.ERROR, "HTTP code = " + code + ", Response = " + response);
      return false;
    } else if (code >= 500 && code < 600) {
      setResult(IOutputHistoryActivity.HTTP_ERROR, Result.ERROR, "Server exception: " + response);
      long currentTime = System.currentTimeMillis();
      throw new ServiceInterruption("Server exception: " + response,
        new ManifoldCFException(response),
        currentTime + 300000L,
        currentTime + 20L * 60000L,
        -1,
        false);
    }
    setResult(IOutputHistoryActivity.HTTP_ERROR, Result.UNKNOWN, "HTTP code = " + code + ", Response = " + response);
    throw new ManifoldCFException("Unexpected HTTP result code: " + code + ": " + response);
  }

  protected void handleHttpException(HttpException e)
    throws ManifoldCFException, ServiceInterruption {
    setResult(e.getClass().getSimpleName().toUpperCase(Locale.ROOT), Result.ERROR, e.getMessage());
    throw new ManifoldCFException(e);
  }

  protected void handleIOException(IOException e)
    throws ManifoldCFException, ServiceInterruption {
    if (e instanceof java.io.InterruptedIOException && !(e instanceof java.net.SocketTimeoutException))
      throw new ManifoldCFException(e.getMessage(), ManifoldCFException.INTERRUPTED);
    setResult(e.getClass().getSimpleName().toUpperCase(Locale.ROOT), Result.ERROR, e.getMessage());
    long currentTime = System.currentTimeMillis();
    // All IO exceptions are treated as service interruptions, retried for an hour
    throw new ServiceInterruption("IO exception: " + e.getMessage(), e,
      currentTime + 60000L,
      currentTime + 1L * 60L * 60000L,
      -1,
      true);
  }

  private static String getResponseBodyAsString(HttpEntity entity)
    throws IOException, HttpException {
    InputStream is = entity.getContent();
    if (is != null) {
      try {
        Charset charSet;
        try {
          ContentType ct = ContentType.get(entity);
          if (ct == null)
            charSet = StandardCharsets.UTF_8;
          else
            charSet = ct.getCharset();
        } catch (ParseException e) {
          charSet = StandardCharsets.UTF_8;
        }
        char[] buffer = new char[65536];
        Reader r = new InputStreamReader(is, charSet);
        Writer w = new StringWriter();
        try {
          while (true) {
            int amt = r.read(buffer);
            if (amt == -1)
              break;
            w.write(buffer, 0, amt);
          }
        } finally {
          w.flush();
        }
        return w.toString();
      } finally {
        is.close();
      }
    }
    return "";
  }

  protected String checkJson(String jsonQuery) throws ManifoldCFException {
    String result = null;
    if (response != null) {
      String[] tokens = response.replaceAll("\\{", "").replaceAll("\\}", "")
        .split(",");
      for (String token : tokens)
        if (token.contains(jsonQuery))
          result = token.substring(token.indexOf(":") + 1);
    }
    return result;
  }

  protected void setResult(String resultCode, Result res, String desc) {
    if (res != null)
      result = res;
    if (desc != null)
      if (desc.length() > 0)
        resultDescription = desc;
    setResultCode(resultCode);
  }

  public String getResultDescription() {
    return resultDescription;
  }

  protected String getResponse() {
    return response;
  }

  public Result getResult() {
    return result;
  }

  public String getCallUrlSnippet() {
    return callUrlSnippet;
  }

  public String getResultCode() {
    return resultCode;
  }

  public void setResultCode(String resultCode) {
    this.resultCode = resultCode;
  }
}
