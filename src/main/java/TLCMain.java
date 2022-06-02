import batch.Application;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Hashtable;

public class TLCMain {

    public static void main(String[] args) {
        Application app = Application.init();
        ArrayList<String> usedColumns = new ArrayList<>();
        //usedColumns.add("tpep_pickup_datetime");
        usedColumns.add("passenger_count");
        usedColumns.add("tpep_dropoff_datetime");
        usedColumns.add("tip_amount");
        usedColumns.add("total_amount");
        usedColumns.add("tolls_amount");

        //"fare_amount"
        //"extra"
        //"improvement_surcharge"
        //"mta_tax"
        //"congestion_surcharge"
        //"airport_fee"
        ArrayList<String> locations = new ArrayList<>();
        locations.add("data/yellow_tripdata_2021-12.parquet");
        locations.add("data/yellow_tripdata_2022-01.parquet");
        locations.add("data/yellow_tripdata_2022-02.parquet");


        try {
            Dataset<Row> dataset = app.preprocessing(locations, usedColumns);
            dataset.show();

            // get mapping between columns and Row indexes
            Hashtable<String, Integer> columns = new Hashtable<>(); //passenger_count|tip_amount| tolls_amount |total_amount |month
            int i = 0;
            for(String c: dataset.columns()){
                columns.put(c, i);
                i +=1;
            }

            JavaRDD<Row> rdd = dataset.toJavaRDD();
            app.query1(rdd, columns);


        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}