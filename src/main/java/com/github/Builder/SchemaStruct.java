package com.github.Builder;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SchemaStruct {
    private static DatasetReader datasetReader;

    public Dataset<Row> parse(String path,String FileTag) {
        final String extension = FilenameUtils.getExtension(path).toUpperCase();
        Dataset<Row> BasicSchema;

        switch (ReaderType.valueOf(ReaderType.class, extension)) {
            case XML:
                datasetReader = new XmlReader(FileTag);
                BasicSchema = datasetReader.extract(path);
                break;
            case JSON:
                datasetReader = new JsonReader();
                BasicSchema = datasetReader.extract(path);
                break;
            default:
                BasicSchema = null;
                break;
        }
        if (BasicSchema == null) {
            throw new IllegalArgumentException("Illegal file Path " + path);
        }
        return BasicSchema;
    }

}
