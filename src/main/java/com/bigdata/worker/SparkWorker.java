package com.bigdata.worker;

import com.bigdata.model.Uservisit;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;

public class SparkWorker {

    private static final String KEYSPACE = "uservisits_scheme";
    private static final String TABLE = "uservisit";
    private static final String MASTER_URL = "spark://darthoo-VirtualBox:7077";
    private static final String APP_NAME = "Cassandra app";

    public void main(String[] args){
        SparkSession sparkSession = createSparkSession();

        CassandraJavaRDD<Uservisit> rdd = javaFunctions(sparkSession.sparkContext())
                .cassandraTable(KEYSPACE, TABLE, mapRowTo(Uservisit.class));
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rdd.rdd(), Uservisit.class);

        Dataset<Row> result = getCountries(sparkSession, dataFrame);
        result.show();
    }

    private Dataset<Row> getCountries(SparkSession sparkSession, Dataset<Row> dataframe) {
        String query = "SELECT * FROM uservisit";
        dataframe.createOrReplaceTempView(TABLE);
        return sparkSession.sql(query).limit(10);
    }

    private SparkSession createSparkSession(){
        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "localhost")
                .setAppName(APP_NAME)
                .setMaster(MASTER_URL);
        return SparkSession.builder().sparkContext(new SparkContext(conf)).getOrCreate();
    }

}
