package io.unitycatalog.spark;

/**
 * Backward-compatible shim: all constants and the {@code createRequestRetryPolicy} method are
 * inherited from the engine-agnostic {@link io.unitycatalog.hadoop.UCHadoopConf}.
 */
public class UCHadoopConf extends io.unitycatalog.hadoop.UCHadoopConf {
  private UCHadoopConf() {}
}
