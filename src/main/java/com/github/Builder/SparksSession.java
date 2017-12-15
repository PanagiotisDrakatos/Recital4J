package com.github.Builder;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public final class SparksSession {
    private static SparksSession instance;
    private static final Logger logger = Logger.getLogger(SparksSession.class.getName());
    private static SparkSession spark_Instance;

    private SparksSession() {
        if (instance != null) {
            throw new IllegalStateException("Already initialized.");
        }
    }

    public static synchronized SparkSession getInstance() {
        synchronized (SparksSession.class) {
            if (instance == null) {
                try {
                    Policies.Path_Setup();
                    spark_Instance = SparkSession
                            .builder()
                            .appName("JavaSparkRecital")
                            .master("local")
                            .getOrCreate();
                    instance = new SparksSession();
                } catch (IOException e) {
                    logger.info("Path Setup failed Plz try again from different OS!");
                }
            }
        }
        return spark_Instance;
    }
}
