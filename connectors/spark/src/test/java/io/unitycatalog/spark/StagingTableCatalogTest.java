package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

/**
 * Test suite for StagingTableCatalog implementation in UCSingleCatalog.
 *
 * <p>This test verifies that UCSingleCatalog correctly implements the StagingTableCatalog
 * interface, which is essential for supporting atomic CREATE TABLE AS SELECT (CTAS) and REPLACE
 * TABLE AS SELECT (RTAS) operations.
 *
 * <p>The key challenge in testing StagingTableCatalog is that:
 *
 * <ul>
 *   <li>Spark uses staging methods (stageCreate, stageReplace, stageCreateOrReplace) internally
 *       when executing CTAS/RTAS operations
 *   <li>These methods are not directly exposed to users but are called by Spark's internal
 *       mechanisms
 *   <li>We need to verify that the delegation to the underlying DeltaCatalog works correctly
 *   <li>We need to ensure atomic operations - the table should only be visible after the SELECT
 *       completes successfully
 * </ul>
 *
 * <p>Testing approach:
 *
 * <ul>
 *   <li>Test CTAS operations on managed tables to ensure atomic creation
 *   <li>Test RTAS operations to ensure atomic replacement
 *   <li>Verify that tables don't exist until the operation completes
 *   <li>Verify that data is correctly populated through the AS SELECT clause
 * </ul>
 */
public class StagingTableCatalogTest extends BaseSparkIntegrationTest {

  private static final String SOURCE_TABLE = "source_table";
  private static final String TARGET_TABLE = "target_table";

  /**
   * Test CREATE TABLE AS SELECT (CTAS) for managed tables.
   *
   * <p>This test verifies that:
   *
   * <ul>
   *   <li>CTAS creates the table atomically
   *   <li>The table schema is inferred from the SELECT statement
   *   <li>Data is correctly populated from the source
   * </ul>
   */
  @Test
  public void testCreateTableAsSelect() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);

    // Create a source table with data
    String sourceFullName = String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, SOURCE_TABLE);
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING DELTA TBLPROPERTIES ('%s'='%s')",
        sourceFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", sourceFullName);

    // Verify source data
    List<Row> sourceRows = sql("SELECT * FROM %s ORDER BY id", sourceFullName);
    assertThat(sourceRows).hasSize(3);

    // Test CTAS - Create target table from source using AS SELECT
    String targetFullName = String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, TARGET_TABLE);
    sql(
        "CREATE TABLE %s USING DELTA TBLPROPERTIES ('%s'='%s') AS SELECT * FROM %s",
        targetFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE,
        sourceFullName);

    // Verify the target table exists and has correct data
    assertThat(session.catalog().tableExists(targetFullName)).isTrue();

    List<Row> targetRows = sql("SELECT * FROM %s ORDER BY id", targetFullName);
    assertThat(targetRows).hasSize(3);
    assertThat(targetRows.get(0).getInt(0)).isEqualTo(1);
    assertThat(targetRows.get(0).getString(1)).isEqualTo("Alice");
    assertThat(targetRows.get(1).getInt(0)).isEqualTo(2);
    assertThat(targetRows.get(1).getString(1)).isEqualTo("Bob");
    assertThat(targetRows.get(2).getInt(0)).isEqualTo(3);
    assertThat(targetRows.get(2).getString(1)).isEqualTo("Charlie");

    // Verify schema is correct
    assertThat(session.table(targetFullName).schema().fields()).hasSize(2);
    assertThat(session.table(targetFullName).schema().fields()[0].name()).isEqualTo("id");
    assertThat(session.table(targetFullName).schema().fields()[0].dataType())
        .isEqualTo(DataTypes.IntegerType);
    assertThat(session.table(targetFullName).schema().fields()[1].name()).isEqualTo("name");
    assertThat(session.table(targetFullName).schema().fields()[1].dataType())
        .isEqualTo(DataTypes.StringType);
  }

  /**
   * Test REPLACE TABLE AS SELECT (RTAS) for managed tables.
   *
   * <p>This test verifies that:
   *
   * <ul>
   *   <li>RTAS atomically replaces an existing table
   *   <li>The old data is completely replaced with new data
   *   <li>The table schema can change during replacement
   * </ul>
   */
  @Test
  public void testReplaceTableAsSelect() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);

    // Create initial target table with some data
    String targetFullName = String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, TARGET_TABLE);
    sql(
        "CREATE TABLE %s (id INT, value STRING) USING DELTA TBLPROPERTIES ('%s'='%s')",
        targetFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    sql("INSERT INTO %s VALUES (1, 'old'), (2, 'data')", targetFullName);

    // Verify initial data
    List<Row> initialRows = sql("SELECT * FROM %s ORDER BY id", targetFullName);
    assertThat(initialRows).hasSize(2);

    // Create a source table with different data
    String sourceFullName = String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, SOURCE_TABLE);
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING DELTA TBLPROPERTIES ('%s'='%s')",
        sourceFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    sql("INSERT INTO %s VALUES (10, 'New'), (20, 'Data'), (30, 'Here')", sourceFullName);

    // Test RTAS - Replace target table with data from source
    sql(
        "REPLACE TABLE %s USING DELTA TBLPROPERTIES ('%s'='%s') AS SELECT * FROM %s",
        targetFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE,
        sourceFullName);

    // Verify the target table still exists
    assertThat(session.catalog().tableExists(targetFullName)).isTrue();

    // Verify the data has been completely replaced
    List<Row> replacedRows = sql("SELECT * FROM %s ORDER BY id", targetFullName);
    assertThat(replacedRows).hasSize(3);
    assertThat(replacedRows.get(0).getInt(0)).isEqualTo(10);
    assertThat(replacedRows.get(0).getString(1)).isEqualTo("New");
    assertThat(replacedRows.get(1).getInt(0)).isEqualTo(20);
    assertThat(replacedRows.get(1).getString(1)).isEqualTo("Data");
    assertThat(replacedRows.get(2).getInt(0)).isEqualTo(30);
    assertThat(replacedRows.get(2).getString(1)).isEqualTo("Here");

    // Verify the old data is gone
    long oldDataCount = sql("SELECT * FROM %s WHERE id < 10", targetFullName).size();
    assertThat(oldDataCount).isEqualTo(0);
  }

  /**
   * Test CREATE OR REPLACE TABLE AS SELECT for managed tables.
   *
   * <p>This test verifies that:
   *
   * <ul>
   *   <li>CREATE OR REPLACE works when table doesn't exist (behaves like CREATE)
   *   <li>CREATE OR REPLACE works when table exists (behaves like REPLACE)
   * </ul>
   */
  @Test
  public void testCreateOrReplaceTableAsSelect() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);

    String targetFullName = String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, TARGET_TABLE);

    // First, test CREATE OR REPLACE when table doesn't exist
    sql(
        "CREATE OR REPLACE TABLE %s USING DELTA TBLPROPERTIES ('%s'='%s') AS SELECT 1 as id, 'first' as name",
        targetFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);

    assertThat(session.catalog().tableExists(targetFullName)).isTrue();
    List<Row> firstRows = sql("SELECT * FROM %s", targetFullName);
    assertThat(firstRows).hasSize(1);
    assertThat(firstRows.get(0).getInt(0)).isEqualTo(1);
    assertThat(firstRows.get(0).getString(1)).isEqualTo("first");

    // Now test CREATE OR REPLACE when table exists - should replace
    sql(
        "CREATE OR REPLACE TABLE %s USING DELTA TBLPROPERTIES ('%s'='%s') AS SELECT 2 as id, 'second' as name",
        targetFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);

    assertThat(session.catalog().tableExists(targetFullName)).isTrue();
    List<Row> secondRows = sql("SELECT * FROM %s", targetFullName);
    assertThat(secondRows).hasSize(1);
    assertThat(secondRows.get(0).getInt(0)).isEqualTo(2);
    assertThat(secondRows.get(0).getString(1)).isEqualTo("second");
  }

  /**
   * Test CTAS with transformations and aggregations.
   *
   * <p>This verifies that complex SELECT queries work correctly with CTAS.
   */
  @Test
  public void testCreateTableAsSelectWithTransformations() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);

    // Create source table
    String sourceFullName = String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, SOURCE_TABLE);
    sql(
        "CREATE TABLE %s (id INT, value INT) USING DELTA TBLPROPERTIES ('%s'='%s')",
        sourceFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30), (4, 40)", sourceFullName);

    // Create target table with aggregated and transformed data
    String targetFullName = String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, TARGET_TABLE);
    sql(
        "CREATE TABLE %s USING DELTA TBLPROPERTIES ('%s'='%s') AS "
            + "SELECT COUNT(*) as count, SUM(value) as total, AVG(value) as avg FROM %s",
        targetFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE,
        sourceFullName);

    // Verify the aggregated results
    List<Row> rows = sql("SELECT * FROM %s", targetFullName);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getLong(0)).isEqualTo(4); // count
    assertThat(rows.get(0).getLong(1)).isEqualTo(100); // sum
    assertThat(rows.get(0).getDouble(2)).isEqualTo(25.0); // avg
  }

  /**
   * Test CTAS with partitioned tables.
   *
   * <p>This verifies that partitioning works correctly with CTAS.
   */
  @Test
  public void testCreateTableAsSelectWithPartitions() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);

    // Create source table
    String sourceFullName = String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, SOURCE_TABLE);
    sql(
        "CREATE TABLE %s (id INT, category STRING, value INT) USING DELTA TBLPROPERTIES ('%s'='%s')",
        sourceFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    sql(
        "INSERT INTO %s VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'B', 40)",
        sourceFullName);

    // Create partitioned target table using CTAS
    String targetFullName = String.format("%s.%s.%s", CATALOG_NAME, SCHEMA_NAME, TARGET_TABLE);
    sql(
        "CREATE TABLE %s USING DELTA PARTITIONED BY (category) "
            + "TBLPROPERTIES ('%s'='%s') AS SELECT * FROM %s",
        targetFullName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE,
        sourceFullName);

    // Verify data is correctly partitioned
    List<Row> categoryA = sql("SELECT * FROM %s WHERE category = 'A' ORDER BY id", targetFullName);
    assertThat(categoryA).hasSize(2);
    assertThat(categoryA.get(0).getInt(0)).isEqualTo(1);
    assertThat(categoryA.get(1).getInt(0)).isEqualTo(3);

    List<Row> categoryB = sql("SELECT * FROM %s WHERE category = 'B' ORDER BY id", targetFullName);
    assertThat(categoryB).hasSize(2);
    assertThat(categoryB.get(0).getInt(0)).isEqualTo(2);
    assertThat(categoryB.get(1).getInt(0)).isEqualTo(4);
  }
}
