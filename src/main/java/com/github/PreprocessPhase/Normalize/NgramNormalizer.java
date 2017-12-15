package com.github.PreprocessPhase.Normalize;

import org.apache.spark.ml.feature.NGram;

public class NgramNormalizer implements Normalizer {
    private String InputColumn;
    private String OutputColum;


    //this class creates trigramms
    public NgramNormalizer(String InputColumn, String OutputColum) {
        this.InputColumn = InputColumn;
        this.OutputColum = OutputColum;
    }

    protected NGram GetModel() {
        return new NGram()
                .setN(2)
                .setInputCol(InputColumn).setOutputCol(OutputColum);
    }

    public String getOutputColum() {
        return OutputColum;
    }

    public String getInputColumn() {
        return InputColumn;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
