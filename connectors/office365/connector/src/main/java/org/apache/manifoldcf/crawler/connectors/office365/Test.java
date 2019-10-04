package org.apache.manifoldcf.crawler.connectors.office365;

import com.microsoft.graph.authentication.IAuthenticationProvider;
import com.microsoft.graph.http.IHttpRequest;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.Site;
import com.microsoft.graph.options.QueryOption;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import com.microsoft.graph.requests.extensions.ISiteCollectionPage;
import com.microsoft.graph.requests.extensions.ISiteCollectionRequest;

import java.util.Collections;

public class Test {
    public static void main(String[] args) {
        IGraphServiceClient graphClient = GraphServiceClient
                .builder()
                .authenticationProvider(new IAuthenticationProvider() {
                    @Override
                    public void authenticateRequest(IHttpRequest iHttpRequest) {
                        iHttpRequest.addHeader("Authorization", "Bearer eyJ0eXAiOiJKV1QiLCJub25jZSI6Ik1uLVFQaG1IVjZReldLaU1JcGMzaUJKZGV1aGhQRXdPS3p6ODVQZ0M2ZWMiLCJhbGciOiJSUzI1NiIsIng1dCI6ImFQY3R3X29kdlJPb0VOZzNWb09sSWgydGlFcyIsImtpZCI6ImFQY3R3X29kdlJPb0VOZzNWb09sSWgydGlFcyJ9.eyJhdWQiOiJodHRwczovL2dyYXBoLm1pY3Jvc29mdC5jb20iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC84YThlZjFhMi1jNjkyLTQ3OGItYjM1ZS0yZWI2NzJkYjA5ZDkvIiwiaWF0IjoxNTY5ODE4MTc5LCJuYmYiOjE1Njk4MTgxNzksImV4cCI6MTU2OTgyMjA3OSwiYWlvIjoiNDJGZ1lEZ2VtcGQyd1d1SFMrZUZXelcrNnk5cEF3QT0iLCJhcHBfZGlzcGxheW5hbWUiOiJyZURvY2sgQ29ubmVjdG9yIiwiYXBwaWQiOiJmOTNmOWZkZC04MzNhLTQzZjAtYTk2MC1mMGY1YzdhN2I5MWQiLCJhcHBpZGFjciI6IjEiLCJpZHAiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC84YThlZjFhMi1jNjkyLTQ3OGItYjM1ZS0yZWI2NzJkYjA5ZDkvIiwib2lkIjoiMWU1ZjFkMjItNmI1My00MjkwLTk0NDYtMmU3ZjkyODkyNmYxIiwicm9sZXMiOlsiU2l0ZXMuUmVhZC5BbGwiLCJGaWxlcy5SZWFkLkFsbCJdLCJzdWIiOiIxZTVmMWQyMi02YjUzLTQyOTAtOTQ0Ni0yZTdmOTI4OTI2ZjEiLCJ0aWQiOiI4YThlZjFhMi1jNjkyLTQ3OGItYjM1ZS0yZWI2NzJkYjA5ZDkiLCJ1dGkiOiJiQ1c4LU5IdUJFaUZtSi1uYTFRZ0FBIiwidmVyIjoiMS4wIiwieG1zX3RjZHQiOjE0Mjc0NDk0NzV9.EYFfBEBQv25LGBUmEoUNVSmFzgF-a6ZNC8NWFX7gHWuqhdht6plZa0HRZyJbe3hqyPYnDzdf2UDKrzyW44SIEzmYiFVwctJRixLox4KL8Zy4AF0qFjKloklw8BmFLUb8kiRMZBA_LvNt0PNnbkSg1JRmIfFsePciHaAA6payfVibA5OcBNN0epKfphXmcPgTFVy1IJlKZP4Ok290ta2mGKITmIBwouXD0cotyTj5yKJaDXyW1UuqGwdT89VxSbl-c59kZblxX2sF0q6iiDADF2xTG0-MhIdczdlm_-R7F9-kkTbNaA4NTf4OeBdNU2kPA-UHofoAE8h6xDdC7MkKqA");
                    }
                })
                // the graph sdk logging sucks, just disable it
                .buildClient();

        ISiteCollectionRequest request = graphClient.sites().buildRequest(Collections.singletonList(new QueryOption("search", "*")));
        ISiteCollectionPage page = request.get();
        System.out.println(page.getCurrentPage());
    }
}
