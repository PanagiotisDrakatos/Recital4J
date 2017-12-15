package com.github.PreprocessPhase.Normalize;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.NGram;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class fitPipeline implements Normalizer {
    private Dataset<Row> dataset;
    private NGram ngramTransformer;
    private RegexTokenizer regexTokenizer;

    //this class creates pipeline from other normalizers
    public fitPipeline(
            Dataset<Row> dataset,
            NGram ngramTransformer,
            RegexTokenizer regexTokenizer) {
        this.dataset = dataset;
        this.ngramTransformer = ngramTransformer;
        this.regexTokenizer = regexTokenizer;
    }

    protected Dataset<Row> GetModel() {
        final Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{regexTokenizer, ngramTransformer});
   //     dataset.show(false);
        return pipeline
                .fit(dataset)
                .transform(dataset);
    }


    @Override
    public String toString() {
        return super.toString();
    }
}
