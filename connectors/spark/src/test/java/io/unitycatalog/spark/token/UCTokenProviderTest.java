package io.unitycatalog.spark.token;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.spark.RetryingApiClient;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.utils.Clock;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class UCTokenProviderTest {

  private static final String OAUTH_URI = "https://oauth.example.com/token";
  private static final String CLIENT_ID = "test-client-id";
  private static final String CLIENT_SECRET = "test-client-secret";
  private static final String ACCESS_TOKEN_1 = "access-token-1";
  private static final String ACCESS_TOKEN_2 = "access-token-2";
  private static final long EXPIRES_IN_SECONDS = 3600L;

  private HttpClient mockHttpClient;
  private RetryingApiClient mockRetryingApiClient;
  private Clock.ManualClock clock;

  @BeforeEach
  public void setUp() {
    mockHttpClient = mock(HttpClient.class);
    mockRetryingApiClient = mock(RetryingApiClient.class);
    when(mockRetryingApiClient.getHttpClient()).thenReturn(mockHttpClient);
    clock = (Clock.ManualClock) Clock.manualClock(Instant.now());
  }

  @Test
  public void testFactoryWithToken() {
    Map<String, String> options = Map.of(OptionsUtil.TOKEN, "test-token");
    UCTokenProvider provider = UCTokenProvider.create(options);

    assertThat(provider).isInstanceOf(FixedUCTokenProvider.class);
    assertThat(provider.accessToken()).isEqualTo("test-token");
  }

  @Test
  public void testFactoryWithEmptyTokenThrows() {
    Map<String, String> options = Map.of(OptionsUtil.TOKEN, "");

    assertThatThrownBy(() -> UCTokenProvider.create(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot determine UCTokenProvider from options");
  }

  @Test
  public void testFactoryWithCompleteOAuthConfig() {
    Map<String, String> options = createOAuthOptions(OAUTH_URI, CLIENT_ID, CLIENT_SECRET);
    UCTokenProvider provider = UCTokenProvider.create(options);

    assertThat(provider).isInstanceOf(OAuthUCTokenProvider.class);
    assertThat(provider.properties())
        .containsEntry(UCHadoopConf.UC_OAUTH_URI, OAUTH_URI)
        .containsEntry(UCHadoopConf.UC_OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(UCHadoopConf.UC_OAUTH_CLIENT_SECRET, CLIENT_SECRET);
  }

  @Test
  public void testFactoryWithIncompleteOAuthConfigThrows() {
    Map<String, String> options = Map.of(OptionsUtil.OAUTH_URI, OAUTH_URI);

    assertThatThrownBy(() -> UCTokenProvider.create(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Incomplete OAuth configuration detected");
  }

  @Test
  public void testFactoryWithNoValidConfigThrows() {
    assertThatThrownBy(() -> UCTokenProvider.create(new HashMap<>()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot determine UCTokenProvider from options");
  }

  @Test
  public void testFactoryTokenTakesPrecedenceOverOAuth() {
    Map<String, String> options = new HashMap<>();
    options.put(OptionsUtil.TOKEN, "fixed-token");
    options.put(OptionsUtil.OAUTH_URI, OAUTH_URI);
    options.put(OptionsUtil.OAUTH_CLIENT_ID, CLIENT_ID);
    options.put(OptionsUtil.OAUTH_CLIENT_SECRET, CLIENT_SECRET);

    UCTokenProvider provider = UCTokenProvider.create(options);

    assertThat(provider).isInstanceOf(FixedUCTokenProvider.class);
    assertThat(provider.accessToken()).isEqualTo("fixed-token");
  }

  @Test
  public void testFixedProviderReturnsToken() {
    FixedUCTokenProvider provider = new FixedUCTokenProvider("my-token");

    assertThat(provider.accessToken()).isEqualTo("my-token");
  }

  @Test
  public void testFixedProviderConsistentAcrossCalls() {
    FixedUCTokenProvider provider = new FixedUCTokenProvider("consistent");

    assertThat(provider.accessToken()).isEqualTo("consistent");
    assertThat(provider.accessToken()).isEqualTo("consistent");
    assertThat(provider.accessToken()).isEqualTo("consistent");
  }

  @Test
  public void testFixedProviderProperties() {
    FixedUCTokenProvider provider = new FixedUCTokenProvider("test-token");

    assertThat(provider.properties())
        .hasSize(1)
        .containsEntry(UCHadoopConf.UC_TOKEN_KEY, "test-token");
  }

  @Test
  public void testFixedProviderStaticFactory() {
    FixedUCTokenProvider provider = FixedUCTokenProvider.create("factory-token");

    assertThat(provider).isNotNull();
    assertThat(provider.accessToken()).isEqualTo("factory-token");
  }

  @Test
  public void testOAuthProviderFetchesToken() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, EXPIRES_IN_SECONDS);
    OAuthUCTokenProvider provider = createOAuthProvider(30L);

    String token = provider.accessToken();

    assertThat(token).isEqualTo(ACCESS_TOKEN_1);
    verifyOAuthRequest(1);
  }

  @Test
  public void testOAuthProviderCachesToken() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, EXPIRES_IN_SECONDS);
    OAuthUCTokenProvider provider = createOAuthProvider(30L);

    String token1 = provider.accessToken();
    String token2 = provider.accessToken();
    String token3 = provider.accessToken();

    assertThat(token1).isEqualTo(ACCESS_TOKEN_1);
    assertThat(token2).isEqualTo(ACCESS_TOKEN_1);
    assertThat(token3).isEqualTo(ACCESS_TOKEN_1);
    verifyOAuthRequest(1);
  }

  @Test
  public void testOAuthProviderRenewsBeforeExpiration() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, 60L);
    OAuthUCTokenProvider provider = createOAuthProvider(30L);

    assertThat(provider.accessToken()).isEqualTo(ACCESS_TOKEN_1);

    clock.sleep(Duration.ofSeconds(29));
    assertThat(provider.accessToken()).isEqualTo(ACCESS_TOKEN_1);
    verifyOAuthRequest(1);

    clock.sleep(Duration.ofSeconds(2));
    mockOAuthResponse(ACCESS_TOKEN_2, 60L);
    assertThat(provider.accessToken()).isEqualTo(ACCESS_TOKEN_2);
    verifyOAuthRequest(2);
  }

  @Test
  public void testOAuthProviderSendsCorrectRequest() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, EXPIRES_IN_SECONDS);
    OAuthUCTokenProvider provider = createOAuthProvider(30L);

    provider.accessToken();

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttpClient).send(requestCaptor.capture(), any());

    HttpRequest request = requestCaptor.getValue();
    assertThat(request.uri().toString()).isEqualTo(OAUTH_URI);
    assertThat(request.method()).isEqualTo("POST");
    assertThat(request.headers().firstValue("Authorization"))
        .isPresent().get().asString().startsWith("Basic ");
    assertThat(request.headers().firstValue("Content-Type"))
        .isPresent().get().isEqualTo("application/x-www-form-urlencoded");
  }

  @Test
  public void testOAuthProviderHandlesHttpError() throws Exception {
    @SuppressWarnings("unchecked")
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("Unauthorized");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    OAuthUCTokenProvider provider = createOAuthProvider(30L);

    assertThatThrownBy(() -> provider.accessToken())
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to renew OAuth token")
        .hasCauseInstanceOf(IOException.class);
  }

  @Test
  public void testOAuthProviderHandlesInterruptedException() throws Exception {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new InterruptedException("Test interruption"));

    OAuthUCTokenProvider provider = createOAuthProvider(30L);

    assertThatThrownBy(() -> provider.accessToken())
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to renew OAuth token");

    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    Thread.interrupted();
  }

  @Test
  public void testOAuthProviderRenewsWhenExpired() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, 10L);
    OAuthUCTokenProvider provider = createOAuthProvider(30L);

    assertThat(provider.accessToken()).isEqualTo(ACCESS_TOKEN_1);

    mockOAuthResponse(ACCESS_TOKEN_2, 10L);
    assertThat(provider.accessToken()).isEqualTo(ACCESS_TOKEN_2);
    verifyOAuthRequest(2);
  }

  @Test
  public void testOAuthProviderProperties() {
    OAuthUCTokenProvider provider = new OAuthUCTokenProvider(OAUTH_URI, CLIENT_ID, CLIENT_SECRET);

    assertThat(provider.properties())
        .hasSize(3)
        .containsEntry(UCHadoopConf.UC_OAUTH_URI, OAUTH_URI)
        .containsEntry(UCHadoopConf.UC_OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(UCHadoopConf.UC_OAUTH_CLIENT_SECRET, CLIENT_SECRET);
  }

  @Test
  public void testOAuthProviderValidatesNullUri() {
    assertThatThrownBy(() -> createOAuthProvider(null, CLIENT_ID, CLIENT_SECRET, 30L))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("OAuth URI must not be null");
  }

  @Test
  public void testOAuthProviderValidatesNullClientId() {
    assertThatThrownBy(() -> createOAuthProvider(OAUTH_URI, null, CLIENT_SECRET, 30L))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("OAuth client ID must not be null");
  }

  @Test
  public void testOAuthProviderValidatesNullClientSecret() {
    assertThatThrownBy(() -> createOAuthProvider(OAUTH_URI, CLIENT_ID, null, 30L))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("OAuth client secret must not be null");
  }

  @Test
  public void testOAuthProviderValidatesNegativeLeadRenewalTime() {
    assertThatThrownBy(() -> createOAuthProvider(OAUTH_URI, CLIENT_ID, CLIENT_SECRET, -1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Lead renewal time must be non-negative");
  }

  @Test
  public void testOAuthProviderAllowsZeroLeadRenewalTime() throws Exception {
    mockOAuthResponse(ACCESS_TOKEN_1, 60L);

    OAuthUCTokenProvider provider = createOAuthProvider(0L);

    assertThat(provider.accessToken()).isEqualTo(ACCESS_TOKEN_1);
  }

  @Test
  public void testOAuthProviderDefaultConstructor() {
    OAuthUCTokenProvider provider = new OAuthUCTokenProvider(OAUTH_URI, CLIENT_ID, CLIENT_SECRET);

    assertThat(provider).isNotNull();
    assertThat(provider.properties()).containsKey(UCHadoopConf.UC_OAUTH_URI);
  }

  private Map<String, String> createOAuthOptions(String uri, String clientId, String secret) {
    return Map.of(
        OptionsUtil.OAUTH_URI, uri,
        OptionsUtil.OAUTH_CLIENT_ID, clientId,
        OptionsUtil.OAUTH_CLIENT_SECRET, secret);
  }

  private OAuthUCTokenProvider createOAuthProvider(long leadRenewalTimeSeconds) {
    return createOAuthProvider(OAUTH_URI, CLIENT_ID, CLIENT_SECRET, leadRenewalTimeSeconds);
  }

  private OAuthUCTokenProvider createOAuthProvider(
      String uri, String clientId, String secret, long leadRenewalTimeSeconds) {
    return new OAuthUCTokenProvider(
        uri, clientId, secret, leadRenewalTimeSeconds, mockRetryingApiClient, clock);
  }

  private void mockOAuthResponse(String accessToken, long expiresIn) throws Exception {
    String jsonResponse = String.format(
        "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":%d}",
        accessToken, expiresIn);

    @SuppressWarnings("unchecked")
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(jsonResponse);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);
  }

  private void verifyOAuthRequest(int times) throws Exception {
    verify(mockHttpClient, times(times))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }
}
