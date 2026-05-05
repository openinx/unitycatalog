package io.unitycatalog.hadoop;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.auth.CredPropsUtil;
import java.util.Collections;
import java.util.Map;

/**
 * Produces the Hadoop configuration properties that a connector (Spark, Flink, Trino, etc.) must
 * inject so that cloud storage can be accessed with Unity-Catalog-vended credentials.
 *
 * <pre>{@code
 * Map<String, String> props = CredentialSetting.builder()
 *     .catalogUri(uri)
 *     .tokenProvider(tokenProvider)
 *     .initialCredentials(creds)
 *     .scheme("s3")
 *     .enableCredentialRenewal(true)
 *     .enableCredentialScopedFs(true)
 *     .existingFsImplProperties(sessionFsImplProps)
 *     .buildForTable(tableId, TableOperation.READ_WRITE);
 * }</pre>
 *
 * @since 0.5.0
 */
public final class CredentialSetting {

  private CredentialSetting() {}

  /** Creates a new {@link Builder}. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Collects credential settings and produces Hadoop configuration properties via {@link
   * #buildForTable} or {@link #buildForPath}.
   *
   * <p>Required fields:
   *
   * <ul>
   *   <li>{@link #catalogUri}
   *   <li>{@link #initialCredentials}
   *   <li>{@link #scheme}
   *   <li>{@link #tokenProvider} — only when {@link #enableCredentialRenewal} is {@code true}
   * </ul>
   */
  public static final class Builder {

    private String catalogUri;
    private TokenProvider tokenProvider;
    private TemporaryCredentials initialCredentials;
    private String scheme;
    private boolean credentialRenewalEnabled = true;
    private boolean credentialScopedFsEnabled = true;
    private Map<String, String> existingFsImplProperties = Collections.emptyMap();

    private Builder() {}

    /**
     * (Required) The Unity Catalog server URI, e.g. {@code "https://host/api/2.1/unity-catalog"}.
     */
    public Builder catalogUri(String uri) {
      this.catalogUri = uri;
      return this;
    }

    /**
     * The token provider for UC authentication. Required by default (since credential renewal is
     * enabled by default); may be {@code null} only when credential renewal is explicitly disabled.
     */
    public Builder tokenProvider(TokenProvider tokenProvider) {
      this.tokenProvider = tokenProvider;
      return this;
    }

    /**
     * (Required) The initial temporary credentials vended by UC (AWS session credentials, GCP OAuth
     * token, or Azure SAS). Typically allocated once by the job driver and propagated to all worker
     * nodes so that each worker reuses the same credential rather than vending a new one.
     */
    public Builder initialCredentials(TemporaryCredentials initialCredentials) {
      this.initialCredentials = initialCredentials;
      return this;
    }

    /**
     * (Required) The storage URI scheme ({@code "s3"}, {@code "gs"}, {@code "abfs"}, or {@code
     * "abfss"}). Unrecognized schemes produce an empty map.
     */
    public Builder scheme(String scheme) {
      this.scheme = scheme;
      return this;
    }

    /**
     * Whether to enable automatic credential renewal (default {@code true}). When enabled,
     * configures a vended-token provider that refreshes credentials before they expire.
     */
    public Builder enableCredentialRenewal(boolean enabled) {
      this.credentialRenewalEnabled = enabled;
      return this;
    }

    /**
     * Whether to enable credential-scoped filesystem caching (default {@code true}). When enabled,
     * overrides {@code fs.<scheme>.impl} so that filesystem instances are reused per credential
     * scope.
     */
    public Builder enableCredentialScopedFs(boolean enabled) {
      this.credentialScopedFsEnabled = enabled;
      return this;
    }

    /**
     * Existing {@code fs.<scheme>.impl} properties from the engine session (default empty map).
     * When credential-scoped FS is enabled, these values are stashed under {@code
     * fs.<scheme>.impl.original} so the wrapper can restore the real delegate.
     */
    public Builder existingFsImplProperties(Map<String, String> props) {
      this.existingFsImplProperties = props;
      return this;
    }

    /**
     * Builds Hadoop properties for a <em>table's</em> storage location.
     *
     * @return unmodifiable map; empty if the scheme is unrecognized
     * @throws IllegalStateException if a required field is missing
     */
    public Map<String, String> buildForTable(String tableId, TableOperation tableOperation) {
      validate();
      return CredPropsUtil.createTableCredProps(
          credentialRenewalEnabled,
          credentialScopedFsEnabled,
          existingFsImplProperties,
          scheme,
          catalogUri,
          tokenProvider,
          tableId,
          tableOperation,
          initialCredentials);
    }

    /**
     * Builds Hadoop properties for an <em>external path</em>.
     *
     * @return unmodifiable map; empty if the scheme is unrecognized
     * @throws IllegalStateException if a required field is missing
     */
    public Map<String, String> buildForPath(String path, PathOperation pathOperation) {
      validate();
      return CredPropsUtil.createPathCredProps(
          credentialRenewalEnabled,
          credentialScopedFsEnabled,
          existingFsImplProperties,
          scheme,
          catalogUri,
          tokenProvider,
          path,
          pathOperation,
          initialCredentials);
    }

    private void validate() {
      if (catalogUri == null) {
        throw new IllegalStateException("catalogUri is required");
      }
      if (credentialRenewalEnabled && tokenProvider == null) {
        throw new IllegalStateException(
            "tokenProvider is required when credential " + "renewal is enabled");
      }
      if (initialCredentials == null) {
        throw new IllegalStateException("initialCredentials is required");
      }
      if (scheme == null) {
        throw new IllegalStateException("scheme is required");
      }
    }
  }
}
