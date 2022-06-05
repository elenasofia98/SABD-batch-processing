import batch.Application;
import batch.ClusterConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;

public class TLCMain {

    public static void main(String[] args) {
        String sparkIP = "spark";
        String sparkPort = "7077";
        String hdfsIP = "namenode";
        String hdfsPort = "9000";

        Application app = Application.init(new ClusterConf(hdfsIP, hdfsPort, sparkIP, sparkPort));
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
        ArrayList<String> filenames = new ArrayList<>();
        filenames.add("/input/yellow_tripdata_2021-12.parquet");
        filenames.add("/input/yellow_tripdata_2022-01.parquet");
        filenames.add("/input/yellow_tripdata_2022-02.parquet");


        try {
            Dataset<Row> dataset = app.preprocessing(filenames, usedColumns);
            dataset.show();


            // get mapping between columns and Row indexes
            /*Hashtable<String, Integer> columns = new Hashtable<>(); //passenger_count|tip_amount| tolls_amount |total_amount |month
            int i = 0;
            for(String c: dataset.columns()){
                columns.put(c, i);
                i +=1;
            }

            JavaRDD<Row> rdd = dataset.toJavaRDD();
            app.query1(rdd, columns);
            */

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}