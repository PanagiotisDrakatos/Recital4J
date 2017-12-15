package com.github.PreprocessPhase.Optimal;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Characteristic {
    Dataset<Row> Optimize(Dataset<Row> df);
}
