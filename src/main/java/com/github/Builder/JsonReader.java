package com.github.Builder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JsonReader implements DatasetReader {
    @Override
    public Dataset<Row> extract(String file) {
        return SparksSession.getInstance().read().format("json").load(file);
    }
}
