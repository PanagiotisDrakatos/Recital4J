package com.github.PreprocessPhase.Normalize;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class NormalizeMaker {
    private String InputColumn;
    private String FixedColumn;
    private String OutputColum;

    private Dataset<Row> dataset;

    private NgramNormalizer ngramNormalizer;
    private WhiteSpaceNormalizer whiteSpaceNormalizer;
    private RegexNormalizer regexNormalizer;
    private fitPipeline fitpipeline;

    public NormalizeMaker(String InputColumn, String OutputColum, Dataset<Row> dataset) {
        this.InputColumn = InputColumn;
        this.OutputColum = OutputColum;
        this.dataset = dataset;
        this.FixedColumn = "Fixed" + InputColumn;
        this.whiteSpaceNormalizer = null;
        this.ngramNormalizer = null;
        this.regexNormalizer = null;
    }


    public NormalizeMaker Replace() {
        this.regexNormalizer = new RegexNormalizer(InputColumn, this.FixedColumn, dataset);
        return this;
    }

    public NormalizeMaker LowerCaseWithReduceSpaces() {
        this.whiteSpaceNormalizer = new WhiteSpaceNormalizer(InputColumn, this.regexNormalizer.toString());
        return this;
    }


    public NormalizeMaker Ngrams() {
        this.ngramNormalizer = new NgramNormalizer(whiteSpaceNormalizer.getOutputColum(), OutputColum);
        return this;
    }

    public Dataset<Row> pipeline() {
        this.fitpipeline = new fitPipeline(
                regexNormalizer.GetModel(),
                ngramNormalizer.GetModel(),
                whiteSpaceNormalizer.GetModel());

        return fitpipeline
                .GetModel()
                .select(col("id"), col(this.FixedColumn).as(InputColumn), col(OutputColum));
    }

}
