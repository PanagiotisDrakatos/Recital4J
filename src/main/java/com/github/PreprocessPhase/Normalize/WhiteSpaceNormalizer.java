package com.github.PreprocessPhase.Normalize;

import org.apache.spark.ml.feature.RegexTokenizer;

public class WhiteSpaceNormalizer implements Normalizer {
    private static final String REGEXX_PATERN = "\\s+";

    private String InputColumn;
    private String OutputColum;


    //this class makes 2 thing reduce whitespaces and convert letters to lowercase
    public WhiteSpaceNormalizer(String InputColumn, String OutputColum) {
        this.InputColumn = InputColumn;
        this.OutputColum = OutputColum;
    }

    protected RegexTokenizer GetModel() {
        return new RegexTokenizer()
                .setInputCol(InputColumn)
                .setOutputCol(OutputColum)
                .setPattern(REGEXX_PATERN)
                .setToLowercase(true);
    }

    public String getOutputColum() {
        return OutputColum;
    }

    public String getInputColumn() {
        return InputColumn;
    }

    @Override
    public String toString() {
        return "WhiteSpaceNormalizer";
    }
}
