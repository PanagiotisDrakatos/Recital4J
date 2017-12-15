package com.github;


import com.github.Builder.SchemaStruct;
import com.github.Builder.SparksSession;
import com.github.PreprocessPhase.Normalize.NormalizeMaker;
import com.github.PreprocessPhase.Optimal.CharacteristicMaker;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class App {
    public static void main(String[] args) {
        SchemaStruct struct = new SchemaStruct();
        String path = "C:\\Users\\User\\Desktop\\SparkPath\\books.xml";
        String FileTag = "book";
        String InputColumn = "Title";
        String OutputColumn = "Trigrams";
        String GroupsColumn = "Title_id";

        Dataset<Row> df = struct.parse(path, FileTag);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        NormalizeMaker maker = new NormalizeMaker(InputColumn, OutputColumn, df.select(col(InputColumn)).toDF());

        Dataset<Row> res = maker
                .Replace()
                .LowerCaseWithReduceSpaces()
                .Ngrams()
                .pipeline();


        CharacteristicMaker characteristicMaker = new CharacteristicMaker(res, InputColumn, OutputColumn);
        Dataset<Row> dfs = characteristicMaker
                .CrossJoin()
                .ReduceGroups(GroupsColumn)
                .Iterations(GroupsColumn)
                .getDataset();


        //  df.show(false);


        SparksSession.getInstance().close();


    }
}
