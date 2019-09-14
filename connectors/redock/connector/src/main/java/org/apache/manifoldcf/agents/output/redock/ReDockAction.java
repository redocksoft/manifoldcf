package org.apache.manifoldcf.agents.output.redock;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.HttpClient;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.apache.http.client.methods.HttpPut;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.manifoldcf.agents.interfaces.RepositoryDocument;
import org.apache.manifoldcf.core.common.Base64;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.core.system.ManifoldCF;
import org.apache.manifoldcf.core.util.URLEncoder;
import org.apache.manifoldcf.crawler.system.Logging;

public class ReDockAction extends ReDockConnection {

  public ReDockAction(HttpClient client, ReDockConfig config)
    throws ManifoldCFException {
    super(config, client);
  }

  public void executeGET(String action)
    throws ManifoldCFException, ServiceInterruption {
    StringBuffer url = getApiUrl();
    HttpGet method = new HttpGet(url.toString() + action);
    call(method);
    String error = checkJson(jsonException);
    if (getResult() == Result.OK && error == null) {
      return;
    }
    setResult("JSONERROR", Result.ERROR, error);
    Logging.connectors.warn("reDock: Commit failed: " + getResponse());
  }

  public void executePUT(String documentURI, RepositoryDocument document)
    throws ManifoldCFException, ServiceInterruption {
    StringBuffer url = getApiUrl();
    HttpPut put = new HttpPut(url.toString());
    put.setEntity(new DocumentRequestEntity(documentURI, document));
    call(put);
    String error = checkJson(jsonException);
    if (getResult() == Result.OK && error == null) {
      return;
    }

    setResult("JSONERROR", Result.ERROR, error);
    Logging.connectors.warn("reDock: Commit failed: " + getResponse());
  }

  public void executeDELETE(String documentURI)
    throws ManifoldCFException, ServiceInterruption {
    StringBuffer url = getApiUrl();
    String uri = URLEncoder.encode(documentURI);
    // The token header is in ReDockConnector and has the client name.
    // Once we support the AADV2 Authentication, we'll have to provide the ClientName and Env in the connection
    HttpDelete del = new HttpDelete(url.toString() + uri);
    call(del);
    String error = checkJson(jsonException);
    if (getResult() == Result.OK || getResult() == Result.NOT_FOUND_ON_ENDPOINT && error == null) {
      return;
    }

    setResult("JSONERROR", Result.ERROR, error);
    Logging.connectors.warn("reDock: Delete failed: " + getResponse());
  }

  public JsonNode getResponseJsonNode()
    throws ManifoldCFException {
    try {
      return objectMapper.readTree(getResponse());
    } catch (Exception e) {
      Logging.connectors.error("Unexpected response format from reDock", e);
      throw new ManifoldCFException("IO exception: " + e.getMessage());
    }
  }

  @Override
  protected void handleIOException(IOException e)
    throws ManifoldCFException, ServiceInterruption {
    // We want a quicker failure here!!
    if (e instanceof java.io.InterruptedIOException && !(e instanceof java.net.SocketTimeoutException)) {
      throw new ManifoldCFException(e.getMessage(), ManifoldCFException.INTERRUPTED);
    }
    setResult(e.getClass().getSimpleName().toUpperCase(Locale.ROOT), Result.ERROR, e.getMessage());
    long currentTime = System.currentTimeMillis();
    // One notification attempt, then we're done.
    throw new ServiceInterruption("IO exception: " + e.getMessage(), e,
      currentTime + 60000L,
      currentTime + 1L * 60L * 60000L,
      1,
      false);
  }

  private class DocumentRequestEntity implements HttpEntity
  {
    private final RepositoryDocument document;
    private final InputStream inputStream;
    private final String documentURI;

    public DocumentRequestEntity(String documentURI, RepositoryDocument document)
    {
      this.documentURI = documentURI;
      this.document = document;
      this.inputStream = document.getBinaryStream();
    }

    @Override
    public boolean isChunked() {
      return false;
    }

    @Override
    @Deprecated
    public void consumeContent()
      throws IOException {
      EntityUtils.consume(this);
    }

    @Override
    public boolean isRepeatable() {
      return false;
    }

    @Override
    public boolean isStreaming() {
      return false;
    }

    @Override
    public InputStream getContent()
      throws IllegalStateException {
      return inputStream;
    }

    @Override
    public void writeTo(OutputStream out)
      throws IOException {
      PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
      try
      {
        pw.print("{");
        // Push custom fields
        Iterator<String> i = document.getFields();
        boolean needComma = false;
        while (i.hasNext()){
          String fieldName = i.next();
          Date[] dateFieldValues = document.getFieldAsDates(fieldName);
          if (dateFieldValues != null)
          {
            needComma = writeField(pw, needComma, fieldName, dateFieldValues);
          }
          else
          {
            String[] fieldValues = document.getFieldAsStrings(fieldName);
            needComma = writeField(pw, needComma, fieldName, fieldValues);
          }
        }

        needComma = writeField(pw, needComma, "primaryPath",  document.getSourcePath().toArray(new String[0]));

        // set by FileRepo and O365 repo
        needComma = writeField(pw, needComma, "rootPath", document.getRootPath().toArray(new String[0]));

        // Push origin URL
        if (documentURI != null)
        {
          needComma = writeField(pw, needComma, "sourceUri",  new String[]{documentURI});
        }

        String manifoldHostName = getHostName();

        // Manifold URI
        // the manifold URI is a pointer to the Manifold instance running, as best as we can understand it
        String manifoldUri;
        try {
          manifoldUri = new URI(
              "manifold",
              manifoldHostName == null ? "" : manifoldHostName,
              "/" + ManifoldCF.getProcessID(),
              null
          ).toString();
        } catch (URISyntaxException e) {
          throw new ManifoldCFException("Bad uri: "+e.getMessage(),e);
        }
        needComma = writeField(pw, needComma, "manifoldUri",  new String[]{manifoldUri});

        // Standard document fields
        final Date createdDate = document.getCreatedDate();
        if (createdDate != null)
        {
          needComma = writeField(pw, needComma, "createdDate", new Date[]{createdDate});
        }
        final Date modifiedDate = document.getModifiedDate();
        if (modifiedDate != null)
        {
          needComma = writeField(pw, needComma, "modifiedDate", new Date[]{modifiedDate});
        }
        final Date indexingDate = document.getIndexingDate();
        if (indexingDate != null)
        {
          needComma = writeField(pw, needComma, "indexingDate", new Date[]{indexingDate});
        }
        final String mimeType = document.getMimeType();
        if (mimeType != null)
        {
          needComma = writeField(pw, needComma, "mimeType", new String[]{mimeType});
        }

        if (inputStream != null) {
          if(needComma){
            pw.print(",");
          }
          // I'm told this is not necessary: see CONNECTORS-690
          //pw.print("\"type\" : \"attachment\",");
          pw.print("\"file\" : {");
            String contentType = document.getMimeType();
            if (contentType != null)
              pw.print("\"_content_type\" : "+jsonStringEscape(contentType)+",");
            String fileName = document.getFileName();
            if (fileName != null)
              pw.print("\"_name\" : "+jsonStringEscape(fileName)+",");
            pw.print(" \"_content\" : \"");
            Base64 base64 = new Base64();
            base64.encodeStream(inputStream, pw);
          pw.print("\"}");
          needComma = true;
        }
        pw.print("}");
      } catch (ManifoldCFException e)
      {
        throw new IOException(e.getMessage());
      } finally
      {
        pw.flush();
        IOUtils.closeQuietly(pw);
      }
    }

    @Override
    public long getContentLength() {
      // Unknown (chunked) length
      return -1L;
    }

    @Override
    public Header getContentType() {
      return new BasicHeader("Content-type","application/json");
    }

    @Override
    public Header getContentEncoding() {
      return null;
    }
  }

  protected static boolean writeField(PrintWriter pw, boolean needComma,
                                      String fieldName, String[] fieldValues)
    throws IOException
  {
    if (fieldValues == null) {
      return needComma;
    }

    if (fieldValues.length == 1){
      if (needComma)
        pw.print(",");
      pw.print(jsonStringEscape(fieldName)+" : "+jsonStringEscape(fieldValues[0]));
      needComma = true;
      return needComma;
    }

    if (fieldValues.length > 1){
      if (needComma)
        pw.print(",");
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      for(int j=0; j<fieldValues.length; j++){
        sb.append(jsonStringEscape(fieldValues[j])).append(",");
      }
      sb.setLength(sb.length() - 1); // discard last ","
      sb.append("]");
      pw.print(jsonStringEscape(fieldName)+" : "+sb.toString());
      needComma = true;
    }
    return needComma;
  }

  protected static String jsonStringEscape(String value)
  {
    StringBuilder sb = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++)
    {
      char x = value.charAt(i);
      if (x == '\n')
        sb.append('\\').append('n');
      else if (x == '\r')
        sb.append('\\').append('r');
      else if (x == '\t')
        sb.append('\\').append('t');
      else if (x == '\b')
        sb.append('\\').append('b');
      else if (x == '\f')
        sb.append('\\').append('f');
      else if (x < 32)
      {
        sb.append("\\u").append(String.format(Locale.ROOT, "%04x", (int)x));
      }
      else
      {
        if (x == '\"' || x == '\\' || x == '/')
          sb.append('\\');
        sb.append(x);
      }
    }
    sb.append("\"");
    return sb.toString();
  }

  protected static String formatAsString(final Date dateValue)
  {
    return DateTimeFormatter.ISO_INSTANT.format(dateValue.toInstant());
  }

  protected static boolean writeField(PrintWriter pw, boolean needComma,
                                      String fieldName, Date[] fieldValues)
    throws IOException
  {
    if (fieldValues == null) {
      return needComma;
    }

    if (fieldValues.length == 1){
      if (needComma)
        pw.print(",");
      pw.print(jsonStringEscape(fieldName)+" : "+jsonStringEscape(formatAsString(fieldValues[0])));
      needComma = true;
      return needComma;
    }

    if (fieldValues.length > 1){
      if (needComma)
        pw.print(",");
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      for(int j=0; j<fieldValues.length; j++){
        sb.append(jsonStringEscape(formatAsString(fieldValues[j]))).append(",");
      }
      sb.setLength(sb.length() - 1); // discard last ","
      sb.append("]");
      pw.print(jsonStringEscape(fieldName)+" : "+sb.toString());
      needComma = true;
    }
    return needComma;
  }

  private String getHostName() {
    String hostName = null;
    try {
      hostName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      /* ignore */
    }

    if (hostName == null) {
      String os = System.getProperty("os.name").toLowerCase();
      hostName = System.getenv(os.contains("win") ? "COMPUTERNAME" : "HOSTNAME");
    }

    return hostName;
  }
}
