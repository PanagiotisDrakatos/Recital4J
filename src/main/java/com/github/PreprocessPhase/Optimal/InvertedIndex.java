package com.github.PreprocessPhase.Optimal;

import static org.apache.spark.sql.functions.*;

import com.github.Builder.SparksSession;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.Arrays;


public class InvertedIndex implements Characteristic {

    private static final String ID_INCREMENT = "id";

    private int numPartions;
    private String InputColumn;
    private String TrigramColumn;


    public InvertedIndex(String InputColumn, String TrigramColumn) {
        this.InputColumn = InputColumn;
        this.TrigramColumn = TrigramColumn;
        this.numPartions = 100;
    }

    ////////////////////////////
    // Performing Cross Join
    ////////////////////////////

    @Override
    public Dataset<Row> Optimize(Dataset<Row> df) {

        //Caching the smaller Dataset, for the cross Join ahead.
        Dataset<Row> keyValuesDS = df
                .select(concat_ws(",", col(TrigramColumn)).as(TrigramColumn)).as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(",")).iterator(),
                        Encoders.STRING()).toDF(TrigramColumn)
                .repartition(numPartions).cache();

        keyValuesDS.take(1); //An action to ensure caching before the join operation.
        // keyValuesDS.show(false);

        //Temporarily disabling auto broadcast join.
        SparksSession.getInstance().conf().set("spark.sql.autoBroadcastJoinThreshold", -1);

        //PerformCrossJoin Operation
        // Filtering out the required rows
        df = df
                .select(col("id"), col(InputColumn))
                .crossJoin(keyValuesDS)
                .filter((FilterFunction<Row>) value -> {
                    return value.getString(1).contains(value.getString(2));
                }).toDF();

        // Re enabling the auto broadcast joins, by setting the default value.
        SparksSession.getInstance().conf().set("spark.sql.autoBroadcastJoinThreshold", 10485760);

        //  df.show(false);
        return df;
    }

    @Override
    public String toString() {
        return "InvertedIndex{}";
    }

}
