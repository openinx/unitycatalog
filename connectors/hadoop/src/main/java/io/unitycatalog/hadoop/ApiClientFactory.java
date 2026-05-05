package io.unitycatalog.hadoop;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.retry.RetryPolicy;
import java.net.URI;

public class ApiClientFactory {

  private ApiClientFactory() {}

  public static ApiClient createApiClient(
      RetryPolicy retryPolicy, URI uri, TokenProvider tokenProvider) {

    ApiClientBuilder builder =
        ApiClientBuilder.create().uri(uri).tokenProvider(tokenProvider).retryPolicy(retryPolicy);

    String javaVersion = getJavaVersion();
    if (javaVersion != null) {
      builder.addAppVersion("Java", javaVersion);
    }

    return builder.build();
  }

  private static String getJavaVersion() {
    try {
      return System.getProperty("java.version");
    } catch (Exception e) {
      return null;
    }
  }
}
