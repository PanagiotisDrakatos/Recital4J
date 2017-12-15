package com.github.PreprocessPhase.Optimal;

import com.github.Builder.SparksSession;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
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
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class OptimumIteration implements Characteristic, Serializable {

    private static final String UDF_NAME1 = "RemoveIndex";
    private static final String UDF_NAME2 = "MaxElementLength";

    private EventEncoder eventEncoder;
    private String InputColumn;
    private String OutputColum;


    public OptimumIteration(String InputColumn, String OutputColum) {
        this.InputColumn = InputColumn;
        this.OutputColum = OutputColum;
        this.eventEncoder = new EventEncoder(InputColumn, OutputColum);
    }

    @Override
    public Dataset<Row> Optimize(Dataset<Row> df) {
        df.show(false);
        Dataset<Row> characteristic = df
                .filter(size(col(eventEncoder.getOutputColum())).equalTo(1))
                .select(col(eventEncoder.getInputColumn()), explode(col(eventEncoder.getOutputColum())).as(Encoders.INT()).as(eventEncoder.getOutputColum()))
                .map((MapFunction<Row, Row>) row -> RowFactory.create(new String[]{(row.getString(0))}, row.getInt(1)), eventEncoder.getRowEncoder())
                .toDF()
                .groupByKey((MapFunction<Row, Integer>) value -> value.getInt(1), Encoders.INT())
                .reduceGroups(((ReduceFunction<Row>) (row1, row2) -> {
                    ArrayList<String> l3 = new ArrayList<String>();
                    l3.addAll(row1.getList(0));
                    l3.addAll(row2.getList(0));
                    Seq<String> s1 = JavaConversions.asScalaBuffer(l3).toSeq();
                    return RowFactory.create(s1, row1.getInt(1));
                }))
                .map((MapFunction<Tuple2<Integer, Row>, Row>) value -> value._2, eventEncoder.getRowEncoder())
                .toDF()
                .withColumn(eventEncoder.InputColumn, callUDF(UDF_NAME2, col(eventEncoder.InputColumn)));

        while (df.select(max(size(col(eventEncoder.OutputColum)))).collectAsList().get(0).getInt(0) > 1) {

            final Dataset<Row> ValuestoRemove = characteristic
                    .agg(collect_set(col(eventEncoder.OutputColum)).as(eventEncoder.OutputColum))
                    .select(col(eventEncoder.OutputColum));
            // df = df
            //           .withColumn(eventEncoder.OutputColum, callUDF(UDF_NAME1, col(eventEncoder.OutputColum), ValuestoRemove.col(eventEncoder.OutputColum)));
            df.show(false);
        }
        return null;
    }


    static {
        UDF1<WrappedArray<Integer>, List<Integer>> RemoveIndex = new UDF1<WrappedArray<Integer>, List<Integer>>() {
            @Override
            public List<Integer> call(WrappedArray<Integer> input) throws Exception {
                List<Integer> newLst = new ArrayList<>(JavaConversions.seqAsJavaList(input));
                newLst.removeIf(x -> x == 3 || x == 4);
                return newLst;
            }
        };
        SparksSession.getInstance().udf().register(UDF_NAME1, RemoveIndex, DataTypes.createArrayType(DataTypes.IntegerType));
    }

    static {
        UDF1<WrappedArray<String>, List<String>> MaxElementLength = new UDF1<WrappedArray<String>, List<String>>() {
            @Override
            public List<String> call(WrappedArray<String> input) throws Exception {
                List<String> newLst = new ArrayList<>(JavaConversions.seqAsJavaList(input));
                String max = Collections.max(newLst, new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.length() - o2.length();
                    }
                });
                newLst.removeIf(x -> !x.equals(max) && newLst.size() > 1);
                return newLst;
            }
        };
        SparksSession.getInstance().udf().register(UDF_NAME2, MaxElementLength, DataTypes.createArrayType(DataTypes.StringType));
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
                    new StructField(InputColumn, DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()),
                    new StructField(OutputColum, DataTypes.IntegerType, false, Metadata.empty()),
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

    @Override
    public String toString() {
        return "OptimumIteration{}";
    }
}
