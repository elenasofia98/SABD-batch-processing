import batch.Application;
import batch.ClusterConf;
import batch.TaxiRoute;
import org.apache.spark.api.java.JavaRDD;

import java.io.*;
import java.util.ArrayList;


public class TLCMain {
    public static void writeTime(String path, long duration1, long duration2, int worker) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter("./times/"+path, true));
        String sep = ",";
        out.write(duration1+sep+duration2+sep+worker+"\n");

        // Closing file connections
        out.close();

    }

    public static void main(String[] args) {
        String sparkIP = "spark";
        String sparkPort = "7077";
        String hdfsIP   = "namenode";
        String hdfsPort = "9000";

        Application app = Application.init(new ClusterConf(hdfsIP, hdfsPort, sparkIP, sparkPort));
        ArrayList<String> dimensions = new ArrayList<>();

        //query 1
        dimensions.add("tpep_dropoff_datetime");
        dimensions.add("tip_amount");
        dimensions.add("total_amount");
        dimensions.add("tolls_amount");
        dimensions.add("payment_type");
        //query 2
        dimensions.add("tpep_pickup_datetime");
        dimensions.add("PULocationID");


        String[] filenames = {
                "/input/yellow_tripdata_2021-12.parquet",
                "/input/yellow_tripdata_2022-01.parquet",
                "/input/yellow_tripdata_2022-02.parquet"
        };

        try {
            JavaRDD<TaxiRoute> rdd = app.load(filenames, dimensions);
            rdd = rdd.cache();
            rdd.take(10).forEach(System.out::println);

            /*
            * Per ogni mese solare, calcolare la percentuale media dell’importo della mancia rispetto al costo effettivo
            * della corsa. Calcolare il costo effettivo della corsa come differenza tra Total amount e Tolls amount
            * ed includere soltanto i pagamenti effettuati con carta di credito. Nell’output indicare anche il numero
            * totale di corse usate per calcolare il valore medio*/
            long start = System.currentTimeMillis();
            app.query1(rdd);
            long end = System.currentTimeMillis();
            System.out.println("---------->Duration in millis: " + (end - start));
            long duration1 = end - start;


            /*
            * Per ogni ora, calcolare la distribuzione in percentuale del numero di corse rispetto alle zone di partenza
            * (campo PULocationID), la mancia media e la sua deviazione standard, il metodo di pagamento piu`diffuso.
            * */
            start = System.currentTimeMillis();
            app.query2(rdd);
            end = System.currentTimeMillis();
            System.out.println("---------->Duration in millis: " + (end - start));
            long duration2 = end - start;

            TLCMain.writeTime("out.csv", duration1, duration2, 2);


            app.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}