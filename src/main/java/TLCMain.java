import batch.Application;
import batch.ClusterConf;
import batch.TaxiRoute;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Hashtable;

public class TLCMain {

    public static void main(String[] args) {
        String sparkIP = "spark";
        String sparkPort = "7077";
        String hdfsIP = "namenode";
        String hdfsPort = "9000";

        Application app = Application.init(new ClusterConf(hdfsIP, hdfsPort, sparkIP, sparkPort));
        ArrayList<String> usedColumns = new ArrayList<>();
        //usedColumns.add("tpep_pickup_datetime");
        //usedColumns.add("passenger_count");
        usedColumns.add("tpep_dropoff_datetime");
        usedColumns.add("tip_amount");
        usedColumns.add("total_amount");
        usedColumns.add("tolls_amount");
        usedColumns.add("payment_type");

        //"fare_amount"
        //"extra"
        //"improvement_surcharge"
        //"mta_tax"
        //"congestion_surcharge"
        //"airport_fee"
        ArrayList<String> filenames = new ArrayList<>();
        filenames.add("/input/yellow_tripdata_2021-12.parquet");
        filenames.add("/input/yellow_tripdata_2022-01.parquet");
        filenames.add("/input/yellow_tripdata_2022-02.parquet");


        try {
            Dataset<TaxiRoute> dataset = app.load(filenames, usedColumns);
            dataset.show();

            JavaRDD<TaxiRoute> rdd = dataset.toJavaRDD();
            rdd.take(10).forEach(System.out::println);

            long start = System.currentTimeMillis();
            app.query1(rdd);
            long end = System.currentTimeMillis();
            System.out.println("---------->Duration in millis: " + (end - start));


        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}