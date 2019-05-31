package org.apache.manifoldcf.crawler.connectors.office365;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.graph.authentication.IAuthenticationProvider;
import com.microsoft.graph.http.IHttpRequest;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;

import java.util.ArrayList;
import java.util.List;

class Office365AuthenticationProvider implements IAuthenticationProvider {
  public ManifoldCFException authenticationException = null;
  private HttpClient client;
  private Office365Config config;
  private ObjectMapper objectMapper = new ObjectMapper();

  private static String office365GrantType = "client_credentials";
  private static String office365Scope = "https://graph.microsoft.com/.default";

  private long tokenExpiryMillis = 0;
  private String cachedToken;

  public Office365AuthenticationProvider( Office365Config config) {
    // Create client to retrieve token from OAuth services
    this.client = HttpClients.createDefault();
    this.config = config;
  }

  public String getAuthUrl() {
    return "https://login.microsoftonline.com/" + config.getTenantId() + "/oauth2/v2.0/token";
  }

  public String getAuthToken() throws ManifoldCFException { return getAuthToken(true); }

  public String getAuthToken(boolean useCache)
    throws ManifoldCFException
  {
    long currentTimeMillis = System.currentTimeMillis();
    if (useCache && cachedToken != null && tokenExpiryMillis - currentTimeMillis > 10000) {
      return cachedToken;
    }

    HttpPost post = new HttpPost(getAuthUrl());

    List<NameValuePair> form = new ArrayList();
    form.add(new BasicNameValuePair("client_id", config.getClientId()));
    form.add(new BasicNameValuePair("client_secret", config.getClientSecret()));
    form.add(new BasicNameValuePair("scope", office365Scope));
    form.add(new BasicNameValuePair("grant_type", office365GrantType));

    try {
      UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
      post.setEntity(entity);
      ResponseHandler<String> responseHandler = httpResponse -> {
        int status = httpResponse.getStatusLine().getStatusCode();
        if (status >= 200 && status < 300) {
          HttpEntity responseEntity = httpResponse.getEntity();
          return responseEntity != null ? EntityUtils.toString(responseEntity) : null;
        } else {
          throw new ClientProtocolException("Unexpected response status: " + status);
        }
      };

      String responseBody = client.execute(post, responseHandler);

      // Get token
      JsonNode jsonResponse = objectMapper.readTree(responseBody);
      cachedToken = jsonResponse.get("access_token").toString().replace("\"", "");
      tokenExpiryMillis = currentTimeMillis + 1000 * Long.parseLong(jsonResponse.get("expires_in").toString());
      return cachedToken;
    }
    catch (Exception e) {
      throw new ManifoldCFException(e.getClass().getCanonicalName() + ": " + e.getMessage(), e, ManifoldCFException.REPOSITORY_CONNECTION_ERROR);
    }
  }

  public void authenticateRequest(final IHttpRequest iHttpRequest)
  {
    try {
      String token = getAuthToken();
      iHttpRequest.addHeader("Authorization", "Bearer " + token);
    }
    catch(ManifoldCFException e)
    {
      authenticationException = e;
    }
  }
}
