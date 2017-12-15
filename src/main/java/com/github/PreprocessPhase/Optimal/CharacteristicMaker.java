package com.github.PreprocessPhase.Optimal;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CharacteristicMaker {

    private Characteristic characteristic;
    private Dataset<Row> dataset;
    private String InputColum;
    private String OuputColumn;


    public CharacteristicMaker(Dataset<Row> dataset,
                               String InputColum,
                               String OuputColumn) {
        this.dataset = dataset;
        this.InputColum = InputColum;
        this.OuputColumn = OuputColumn;
    }

    //change State of class
    public CharacteristicMaker CrossJoin() {
        this.characteristic = new InvertedIndex(InputColum, OuputColumn);
        this.ChangeState();
        return this;

    }

    //change State of class
    public CharacteristicMaker Iterations(String columns) {
        this.characteristic = new OptimumIteration(OuputColumn, columns);
        this.ChangeState();
        return this;
    }

    public CharacteristicMaker ReduceGroups(String columns) {
        this.characteristic = new ReduceGroups(OuputColumn, columns);
        this.ChangeState();
        return this;
    }

    private void ChangeState() {
        this.dataset = this.characteristic.Optimize(dataset);
    }

    public Dataset<Row> getDataset() {
        return dataset;
    }
}
