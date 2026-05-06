package io.unitycatalog.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class HadoopCredentialConfTest {

  private static final String S3A_FS = "org.apache.hadoop.fs.s3a.S3AFileSystem";
  private static final String GCS_FS = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
  private static final String ABFS_FS = "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem";
  private static final String ABFSS_FS =
      "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem";

  // ------- S3 -------

  @Test
  void s3TableStaticCredentials() {
    Map<String, String> props =
        staticBuilder("s3")
            .initialCredentials(s3Creds())
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.s3a.access.key", "ak")
        .containsEntry("fs.s3a.secret.key", "sk")
        .containsEntry("fs.s3a.session.token", "st");
  }

  @Test
  void s3TableWithCredScopedFs() {
    Map<String, String> props =
        staticBuilder("s3")
            .initialCredentials(s3Creds())
            .enableCredentialScopedFs(true)
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.s3.impl.original", S3A_FS)
        .containsEntry("fs.s3a.impl.original", S3A_FS);
  }

  @Test
  void s3TableWithCustomFsImpl() {
    Configuration conf = new Configuration(false);
    conf.set("fs.s3.impl", "com.example.Custom");
    conf.set("fs.s3a.impl", "com.example.Custom");

    Map<String, String> props =
        staticBuilder("s3")
            .initialCredentials(s3Creds())
            .enableCredentialScopedFs(true)
            .hadoopConf(conf)
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.s3.impl.original", "com.example.Custom")
        .containsEntry("fs.s3a.impl.original", "com.example.Custom");
  }

  @Test
  void s3PathCredentials() {
    Map<String, String> props =
        staticBuilder("s3")
            .initialCredentials(s3Creds())
            .buildForPath("s3://bucket/path", PathOperation.PATH_READ);

    assertThat(props).containsEntry("fs.s3a.access.key", "ak");
  }

  // ------- GCS -------

  @Test
  void gsTableStaticCredentials() {
    Map<String, String> props =
        staticBuilder("gs")
            .initialCredentials(gcsCreds())
            .buildForTable("tid", TableOperation.READ);

    assertThat(props).containsEntry("fs.gs.auth.access.token.credential", "gcs-token");
  }

  @Test
  void gsTableWithCredScopedFs() {
    Map<String, String> props =
        staticBuilder("gs")
            .initialCredentials(gcsCreds())
            .enableCredentialScopedFs(true)
            .buildForTable("tid", TableOperation.READ);

    assertThat(props).containsEntry("fs.gs.impl.original", GCS_FS);
  }

  // ------- Azure -------

  @Test
  void abfsTableStaticCredentials() {
    Map<String, String> props =
        staticBuilder("abfs")
            .initialCredentials(abfsCreds())
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.azure.sas.fixed.token", "sas-token")
        .containsEntry("fs.azure.account.auth.type", "SAS");
  }

  @Test
  void abfssTableWithCredScopedFs() {
    Map<String, String> props =
        staticBuilder("abfss")
            .initialCredentials(abfsCreds())
            .enableCredentialScopedFs(true)
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.abfs.impl.original", ABFS_FS)
        .containsEntry("fs.abfss.impl.original", ABFSS_FS);
  }

  // ------- Edge cases -------

  @Test
  void unknownSchemeReturnsEmptyMap() {
    Map<String, String> props =
        staticBuilder("hdfs")
            .initialCredentials(s3Creds())
            .buildForTable("tid", TableOperation.READ);

    assertThat(props).isEmpty();
  }

  @Test
  void credScopedFsDisabledNoOriginalKeys() {
    Map<String, String> props =
        staticBuilder("s3")
            .initialCredentials(s3Creds())
            .enableCredentialScopedFs(false)
            .buildForTable("tid", TableOperation.READ);

    assertThat(props)
        .doesNotContainKey("fs.s3.impl.original")
        .doesNotContainKey("fs.s3a.impl.original");
  }

  @Test
  void returnedMapIsUnmodifiable() {
    Map<String, String> props =
        staticBuilder("s3").initialCredentials(s3Creds()).buildForTable("tid", TableOperation.READ);

    assertThatThrownBy(() -> props.put("k", "v")).isInstanceOf(UnsupportedOperationException.class);
  }

  // ------- Validation -------

  @Test
  void missingCatalogUriThrows() {
    assertThatThrownBy(() -> HadoopCredentialConf.builder(null, "s3"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalogUri");
  }

  @Test
  void missingSchemeThrows() {
    assertThatThrownBy(() -> HadoopCredentialConf.builder("http://uc", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scheme");
  }

  @Test
  void missingInitialCredentialsThrows() {
    assertThatThrownBy(() -> staticBuilder("s3").buildForTable("tid", TableOperation.READ))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("initialCredentials");
  }

  @Test
  void missingTokenProviderWithRenewalThrows() {
    assertThatThrownBy(
            () ->
                HadoopCredentialConf.builder("http://uc", "s3")
                    .initialCredentials(s3Creds())
                    .buildForTable("tid", TableOperation.READ))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("tokenProvider");
  }

  // ------- Helpers -------

  /** Builder with credential renewal disabled (static creds). */
  private static HadoopCredentialConf.Builder staticBuilder(String scheme) {
    return HadoopCredentialConf.builder("http://uc", scheme).enableCredentialRenewal(false);
  }

  private static TemporaryCredentials s3Creds() {
    return new TemporaryCredentials()
        .awsTempCredentials(
            new AwsCredentials().accessKeyId("ak").secretAccessKey("sk").sessionToken("st"));
  }

  private static TemporaryCredentials gcsCreds() {
    return new TemporaryCredentials()
        .gcpOauthToken(new GcpOauthToken().oauthToken("gcs-token"))
        .expirationTime(Long.MAX_VALUE);
  }

  private static TemporaryCredentials abfsCreds() {
    return new TemporaryCredentials()
        .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sas-token"));
  }
}
