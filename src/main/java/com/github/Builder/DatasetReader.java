package com.github.Builder;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DatasetReader {
    Dataset<Row> extract(String file);
}
