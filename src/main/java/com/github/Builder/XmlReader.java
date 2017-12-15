package com.github.Builder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;

public class XmlReader implements DatasetReader {
    private static final String databricks = "com.databricks.spark.xml";

    private final String FileTag;

    public XmlReader(String FileTag) {
        this.FileTag = FileTag;
    }

    @Override
    public Dataset<Row> extract(String file) {
        final HashMap<String, String> options = new HashMap<String, String>();
        options.put("rowTag", FileTag);
        options.put("path", file);
        return new SQLContext(SparksSession.getInstance()).load("com.databricks.spark.xml", options);
    }
}
