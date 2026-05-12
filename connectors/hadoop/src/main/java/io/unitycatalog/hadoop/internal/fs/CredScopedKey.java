package io.unitycatalog.hadoop.internal.fs;

import io.unitycatalog.hadoop.UCDeltaTableIdentifier;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Cache key that identifies a credential scope for {@link CredScopedFileSystem}.
 *
 * <p>There are three implementations:
 *
 * <ul>
 *   <li>{@link TableCredScopedKey} — keyed by table ID and operation; used for table-level
 *       temporary credentials via the UC credentials API.
 *   <li>{@link DeltaTableCredScopedKey} — keyed by table identity, operation, and a per-job UID;
 *       used for table-level temporary credentials via the UC Delta credentials API.
 *   <li>{@link PathCredScopedKey} — keyed by path and operation; used for path-level temporary
 *       credentials.
 *   <li>{@link DefaultCredScopedKey} — keyed by URI scheme and authority; used as a fallback when
 *       no Unity Catalog credential type is present in the configuration.
 * </ul>
 */
public interface CredScopedKey {

  static CredScopedKey create(URI uri, Configuration conf) {
    String type = conf.get(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY);
    if (UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      return new PathCredScopedKey(
          conf.get(UCHadoopConfConstants.UC_PATH_KEY),
          conf.get(UCHadoopConfConstants.UC_PATH_OPERATION_KEY));
    } else if (UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      if (conf.getBoolean(
          UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY,
          UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_DEFAULT_VALUE)) {
        return new DeltaTableCredScopedKey(
            UCDeltaTableIdentifier.of(
                conf.get(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY),
                conf.get(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY),
                conf.get(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY)),
            conf.get(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY),
            conf.get(UCHadoopConfConstants.UC_CREDENTIALS_UID_KEY));
      }
      return new TableCredScopedKey(
          conf.get(UCHadoopConfConstants.UC_TABLE_ID_KEY),
          conf.get(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY));
    }

    return new DefaultCredScopedKey(uri, conf);
  }

  class PathCredScopedKey implements CredScopedKey {
    private final String path;
    private final String pathOperation;

    public PathCredScopedKey(String path, String pathOperation) {
      this.path = path;
      this.pathOperation = pathOperation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof PathCredScopedKey)) return false;
      PathCredScopedKey that = (PathCredScopedKey) o;
      return Objects.equals(path, that.path) && Objects.equals(pathOperation, that.pathOperation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, pathOperation);
    }

    @Override
    public String toString() {
      return "PathCredScopedKey{path=" + path + ", op=" + pathOperation + "}";
    }
  }

  class TableCredScopedKey implements CredScopedKey {
    private final String tableId;
    private final String tableOperation;

    public TableCredScopedKey(String tableId, String tableOperation) {
      this.tableId = tableId;
      this.tableOperation = tableOperation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TableCredScopedKey)) return false;
      TableCredScopedKey that = (TableCredScopedKey) o;
      return Objects.equals(tableId, that.tableId)
          && Objects.equals(tableOperation, that.tableOperation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableId, tableOperation);
    }

    @Override
    public String toString() {
      return "TableCredScopedKey{tableId=" + tableId + ", op=" + tableOperation + "}";
    }
  }

  class DeltaTableCredScopedKey implements CredScopedKey {
    private final UCDeltaTableIdentifier identifier;
    private final String tableOperation;
    private final String uid;

    public DeltaTableCredScopedKey(
        UCDeltaTableIdentifier identifier, String tableOperation, String uid) {
      this.identifier = identifier;
      this.tableOperation = tableOperation;
      this.uid = uid;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DeltaTableCredScopedKey)) return false;
      DeltaTableCredScopedKey that = (DeltaTableCredScopedKey) o;
      return Objects.equals(identifier, that.identifier)
          && Objects.equals(tableOperation, that.tableOperation)
          && Objects.equals(uid, that.uid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, tableOperation, uid);
    }

    @Override
    public String toString() {
      return "DeltaTableCredScopedKey{table="
          + identifier
          + ", op="
          + tableOperation
          + ", uid="
          + uid
          + "}";
    }
  }

  class DefaultCredScopedKey implements CredScopedKey {
    private final String scheme;
    private final String authority;

    public DefaultCredScopedKey(URI uri, Configuration conf) {
      if (uri.getScheme() == null && uri.getAuthority() == null) {
        URI defaultUri = FileSystem.getDefaultUri(conf);
        this.scheme = defaultUri.getScheme();
        this.authority = defaultUri.getAuthority();
      } else {
        this.scheme = uri.getScheme();
        this.authority = uri.getAuthority();
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DefaultCredScopedKey)) return false;
      DefaultCredScopedKey that = (DefaultCredScopedKey) o;
      return Objects.equals(scheme, that.scheme) && Objects.equals(authority, that.authority);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scheme, authority);
    }

    @Override
    public String toString() {
      return "DefaultCredScopedKey{scheme=" + scheme + ", authority=" + authority + "}";
    }
  }
}
