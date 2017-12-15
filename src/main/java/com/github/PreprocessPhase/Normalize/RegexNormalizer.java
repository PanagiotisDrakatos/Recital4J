package com.github.PreprocessPhase.Normalize;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



public class RegexNormalizer implements Normalizer {
    private static final String REGEXX_PATERN = "[^\\w\\s]|_";
    private static final String ID_INCREMENT = "id";
    private Dataset<Row> dataset;
    private String InputColumn;
    private String OutputColum;

    //this class replaces punction marks
    public RegexNormalizer(String InputColumn, String OutputColum, Dataset<Row> dataset) {
        this.InputColumn = InputColumn;
        this.OutputColum = OutputColum;
        this.dataset = dataset;
    }

    protected Dataset<Row> GetModel() {
        return dataset
                .withColumn(InputColumn, regexp_replace(col(InputColumn), REGEXX_PATERN, "_"))
                .withColumn(OutputColum, regexp_replace(lower(col(InputColumn)), "\\s+", " "))
                .withColumn(ID_INCREMENT, lit(monotonically_increasing_id())).as(ID_INCREMENT);
    }

    public String getOutputColum() {
        return OutputColum;
    }

    public String getInputColumn() {
        return InputColumn;
    }

    @Override
    public String toString() {
        return "RegexNormalizer";
    }

}
