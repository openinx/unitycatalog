package io.unitycatalog.spark;

import java.util.Map;

public class OAuthUCTokenProvider implements UCTokenProvider {
  private final String oauthUri;
  private final String oauthClientId;
  private final String oauthClientSecret;

  public OAuthUCTokenProvider(String oauthUri, String oauthClientId, String oauthClientSecret) {
    this.oauthUri = oauthUri;
    this.oauthClientId = oauthClientId;
    this.oauthClientSecret = oauthClientSecret;
  }

  @Override
  public String accessToken() {
    return "";
  }

  @Override
  public Map<String, String> properties() {
    return Map.of(
        UCHadoopConf.UC_OAUTH_URI, oauthUri,
        UCHadoopConf.UC_OAUTH_CLIENT_ID, oauthClientId,
        UCHadoopConf.UC_OAUTH_CLIENT_SECRET, oauthClientSecret);
  }
}
