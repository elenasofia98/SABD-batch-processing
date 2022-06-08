import batch.Application;
import batch.ClusterConf;
import batch.TaxiRoute;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.Function1;
import scala.collection.immutable.Seq;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.TreeSet;

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

        //query 1
        usedColumns.add("tpep_dropoff_datetime");
        usedColumns.add("tip_amount");
        usedColumns.add("total_amount");
        usedColumns.add("tolls_amount");
        usedColumns.add("payment_type");

        //query 2
        usedColumns.add("tpep_pickup_datetime");
        usedColumns.add("PULocationID");


        //"airport_fee"
        ArrayList<String> filenames = new ArrayList<>();
        filenames.add("/input/yellow_tripdata_2021-12.parquet");
        filenames.add("/input/yellow_tripdata_2022-01.parquet");
        filenames.add("/input/yellow_tripdata_2022-02.parquet");


        try {
            Dataset<TaxiRoute> dataset = app.load(filenames, usedColumns);
            dataset = dataset.cache();
            dataset.show();

            JavaRDD<TaxiRoute> rdd = dataset.toJavaRDD();
            rdd.cache();
            rdd.take(10).forEach(System.out::println);

            long start = System.currentTimeMillis();
            /*Per ogni mese solare, calcolare la percentuale media dell’importo della mancia rispetto al costo effettivo
            * della corsa. Calcolare il costo effettivo della corsa come differenza tra Total amount e Tolls amount
            * ed includere soltanto i pagamenti effettuati con carta di credito. Nell’output indicare anche il numero
            * totale di corse usate per calcolare il valore medio*/
            app.query1(rdd);
            long end = System.currentTimeMillis();
            System.out.println("---------->Duration in millis: " + (end - start));

            /*
            * Per ogni ora, calcolare la distribuzione in percentuale del numero di corse rispetto alle zone di partenza
            * (campo PULocationID), la mancia media e la sua deviazione standard, il metodo di pagamento piu`diffuso.
            * */
            start = System.currentTimeMillis();
            app.query2(rdd);
            end = System.currentTimeMillis();
            System.out.println("---------->Duration in millis: " + (end - start));

            app.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}