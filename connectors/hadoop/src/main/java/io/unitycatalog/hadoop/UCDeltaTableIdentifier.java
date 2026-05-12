package io.unitycatalog.hadoop;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class UCDeltaTableIdentifier {
  private final String catalog;
  private final String schema;
  private final String table;

  private UCDeltaTableIdentifier(String catalog, String schema, String table) {
    Preconditions.checkArgument(catalog != null && !catalog.isEmpty(), "catalog is required");
    Preconditions.checkArgument(schema != null && !schema.isEmpty(), "schema is required");
    Preconditions.checkArgument(table != null && !table.isEmpty(), "table is required");
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
  }

  public static UCDeltaTableIdentifier of(String catalog, String schema, String table) {
    return new UCDeltaTableIdentifier(catalog, schema, table);
  }

  public String catalog() {
    return catalog;
  }

  public String schema() {
    return schema;
  }

  public String table() {
    return table;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(catalog, schema, table);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof UCDeltaTableIdentifier)) {
      return false;
    } else if (this == o) {
      return true;
    }
    UCDeltaTableIdentifier that = (UCDeltaTableIdentifier) o;
    return Objects.equal(catalog, that.catalog)
        && Objects.equal(schema, that.schema)
        && Objects.equal(table, that.table);
  }

  @Override
  public String toString() {
    return String.format("%s.%s.%s", catalog, schema, table);
  }
}
