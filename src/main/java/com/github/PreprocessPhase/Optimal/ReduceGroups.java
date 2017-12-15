package com.github.PreprocessPhase.Optimal;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.HashSet;
import java.util.Set;


public class ReduceGroups implements Characteristic, Serializable {

    private final EventEncoder eventEncoder;
    private String InputColumn;
    private String OutputColum;


    public ReduceGroups(String InputColumn, String OutputColum) {
        this.InputColumn = InputColumn;
        this.OutputColum = OutputColum;
        this.eventEncoder = new EventEncoder(InputColumn, OutputColum);
    }


    @Override
    public Dataset<Row> Optimize(Dataset<Row> df) {
        return df
                .map((MapFunction<Row, Row>) row -> RowFactory.create(row.getString(2), new long[]{row.getLong(0)}), eventEncoder.getRowEncoder())
                .toDF()
                .groupByKey((MapFunction<Row, String>) value -> value.getString(0), Encoders.STRING())
                .reduceGroups((ReduceFunction<Row>) (row1, row2) -> {
                    Set<Integer> l3 = new HashSet<Integer>();
                    l3.addAll(row1.getList(1));
                    l3.addAll(row2.getList(1));
                    Seq<Integer> s1 = JavaConversions.asScalaSet(l3).toSeq();
                    return RowFactory.create(row1.getString(0), s1);
                })
                .map((MapFunction<Tuple2<String, Row>, Row>) value -> value._2, eventEncoder.getRowEncoder()).toDF();

    }

    @Override
    public String toString() {
        return "ReduceGroups{}";
    }

    public static class EventEncoder {

        private final String InputColumn;
        private final String OutputColum;
        private ExpressionEncoder<Row> rowEncoder;

        public EventEncoder(String InputColumn, String OutputColum) {
            this.InputColumn = InputColumn;
            this.OutputColum = OutputColum;
            this.Init();
        }

        private void Init() {
            final StructType structType = new StructType(new StructField[]{
                    new StructField(InputColumn, DataTypes.StringType, false, Metadata.empty()),
                    new StructField(OutputColum, DataTypes.createArrayType(DataTypes.IntegerType), false, Metadata.empty()),
            });
            this.rowEncoder = RowEncoder.apply(structType);
        }

        public String getInputColumn() {
            return InputColumn;
        }

        public String getOutputColum() {
            return OutputColum;
        }

        public ExpressionEncoder<Row> getRowEncoder() {
            return rowEncoder;
        }

        @Override
        public String toString() {
            return "EventEncoder{" +
                    "InputColumn='" + InputColumn + '\'' +
                    ", OutputColum='" + OutputColum + '\'' +
                    ", rowEncoder=" + rowEncoder +
                    '}';
        }
    }
}
