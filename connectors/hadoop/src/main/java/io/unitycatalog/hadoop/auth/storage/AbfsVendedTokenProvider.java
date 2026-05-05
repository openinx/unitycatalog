package io.unitycatalog.hadoop.auth.storage;

import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.hadoop.UCHadoopConf;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class AbfsVendedTokenProvider extends GenericCredentialProvider implements SASTokenProvider {
  public static final String ACCESS_TOKEN_KEY = "fs.azure.sas.fixed.token";

  public AbfsVendedTokenProvider() {}

  @Override
  public void initialize(Configuration conf, String accountName) {
    initialize(conf);
  }

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    if (conf.get(UCHadoopConf.AZURE_INIT_SAS_TOKEN) != null
        && conf.get(UCHadoopConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME) != null) {

      String sasToken = conf.get(UCHadoopConf.AZURE_INIT_SAS_TOKEN);
      Objects.requireNonNull(
          sasToken,
          String.format(
              "Azure SAS token not set, please check '%s' in hadoop configuration",
              UCHadoopConf.AZURE_INIT_SAS_TOKEN));

      long expiredTimeMillis = conf.getLong(UCHadoopConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME, 0L);
      if (expiredTimeMillis <= 0) {
        throw new IllegalStateException(
            String.format(
                "Azure SAS token expired time must be greater than 0, please check '%s' in hadoop "
                    + "configuration",
                UCHadoopConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME));
      }

      return GenericCredential.forAzure(sasToken, expiredTimeMillis);
    } else {
      return null;
    }
  }

  @Override
  public String getSASToken(String account, String fileSystem, String path, String operation) {
    GenericCredential generic = accessCredentials();

    AzureUserDelegationSAS azureSAS = generic.temporaryCredentials().getAzureUserDelegationSas();
    Objects.requireNonNull(azureSAS, "Azure SAS of generic credential cannot be null");

    return azureSAS.getSasToken();
  }
}
