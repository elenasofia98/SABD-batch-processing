package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class Application {
    protected SparkSession ss;
    protected String hdfs;



    public static Application init(ClusterConf clusterConf){
        SparkConf conf = new SparkConf()
                .setMaster("spark://"+clusterConf.getSparkIP()+":"+clusterConf.getSparkPort())
                .setAppName("TLC-batch-processing");

        SparkSession ss = SparkSession.builder().config(conf).getOrCreate();

        return new Application(ss, clusterConf.getHdfsIP(), clusterConf.getHdfsPort());
    }

    public Application(SparkSession ss, String ip, String port){
        this.ss = ss;
        this.hdfs = "hdfs://"+ip+":"+port;
    }

    private Dataset<Row> load(ArrayList<String> paths) throws Exception {
        Dataset<Row> dataset = null;
        for(String path: paths) {
            Dataset<Row> temp = this.ss.read().parquet(this.hdfs+path);

            if(dataset != null){
                if(Arrays.equals(dataset.columns(), temp.columns()))
                    dataset = dataset.union(temp);
                else {
                    //Check dataset columns
                    throw new Exception("Incompatible columns");
                }
            }
            else
                dataset = temp;
        }

        return dataset;
    }


    public Dataset<Row> preprocessing(ArrayList<String> pahts, ArrayList<String> usedColumns) throws Exception {

        Dataset<Row> dataset = this.load(pahts);
        for(String c: dataset.columns()){
            // System.out.println(c);
            if(!usedColumns.contains(c))
                dataset = dataset.drop(c);
        }

        //dataset = new DataFrameNaFunctions(dataset).drop();

        // tip_amount(double)| tolls_amount(double)|total_amount(double) |month (int)
        dataset = dataset
                .withColumn("date",
                        date_format(dataset.col("tpep_dropoff_datetime"), "yyyy/MM/dd"))
                .filter(col("date").gt(lit("2021/11/30")).and(col("date").lt(lit("2022/03/01"))))
                .drop("tpep_dropoff_datetime")
                .withColumn("passenger_count", dataset.col("passenger_count").cast("int"))
                .withColumn("tip_amount", dataset.col("tip_amount").cast("double"))
                .withColumn("tolls_amount", dataset.col("tolls_amount").cast("double"))
                .withColumn("total_amount", dataset.col("total_amount").cast("double"));
        dataset.cache();

        return dataset;
    }

    /*public void query1(ArrayList<String> usedColumns) throws Exception {
        // Query 1: Averages on monthly basis
        ArrayList<String> locations = new ArrayList<>();
        locations.add("data/yellow_tripdata_2021-12.parquet");
        locations.add("data/yellow_tripdata_2022-01.parquet");
        locations.add("data/yellow_tripdata_2022-02.parquet");

        Dataset<Row> dataset = this.preprocessing(locations, usedColumns);
        dataset.show();
        Hashtable<String, Integer> columns = new Hashtable<>(); //passenger_count|tip_amount| tolls_amount |total_amount |month
        int i = 0;
        for(String c: dataset.columns()){
            System.out.println(c+ " "+ i);
            columns.put(c, i);
            i +=1;
        }


        JavaRDD<Row> rdd = dataset.toJavaRDD();
        int[] months = {1, 2, 12};

        for(int mm: months){
            JavaRDD<Row> by_month  = rdd.filter(row -> (int)row.get(columns.get("month")) == mm).cache();
            //by_month.take(10).forEach(System.out::println);

            // Query 1.1: Average # of passengers by month
            //JavaRDD<Long> tot_passenger = by_month.map(row -> new Long((int) row.get(columns.get("passenger_count"))));
            //tot_passenger.take(1000).forEach(System.out::println);

            long rides = by_month.count();
            System.out.println("--------------------------------");
            System.out.println("Ratio by month: " +
                    " month = " + mm +
                    //" tot_passenger = " + tot_passenger +
                    ", n = " + rides
                    //", avg = " +  tot_passenger/rides
                    );
            System.out.println("--------------------------------");


            /*int count = 0;
            int len = 0;
            for(Double dd: d.collect()){
                if(dd == 0)
                    count += 1;
                len +=1;
            }
            System.out.println("TANAAAAAAAAAAAAAAAA den==0 :"+ count +" vs tot: "+ len);*/
            /* JavaRDD<Tuple2<Double, Double>> ratio = money.map(row -> new Tuple2<>((double) row.get(0),
                    (double) row.get(2) - (double) row.get(1))).filter(tuple -> tuple._2() != 0);

            double sum = ratio.map(tuple -> tuple._1() / tuple._2()).reduce((r1, r2) -> r1 + r2);

            // Query 1.2: calculate average of ratio between tip amount and the difference between total and toll
            JavaRDD<Row> valid = by_month.filter(row -> (double) row.get(columns.get("total_amount")) - (double) row.get(columns.get("tolls_amount")) != 0);
            double sum = valid.map(row ->
                            (double) row.get(columns.get("tip_amount")) / ((double) row.get(columns.get("total_amount")) - (double) row.get(columns.get("tolls_amount"))) )
                    .reduce((r1, r2) -> r1+r2);
            long n = valid.count();

            System.out.println("--------------------------------");
            System.out.println("Ratio by month: " +
                    " month = " + mm +
                    ", sum = " + sum +
                    ", len = " +  n +
                    ", avg = " +  sum/n);
            System.out.println("--------------------------------");
        }




    }*/

    public void query1(JavaRDD<Row> rdd, Hashtable<String, Integer> columns) throws Exception {
        // Query 1: Averages on monthly basis
        JavaPairRDD<String, Row> by_month  = rdd.mapToPair(row -> new Tuple2<>(
                ((String)row.get(columns.get("date"))),
                row)
        );
        //by_month= by_month.cache();
        by_month.collect();

        //TODO
        // Query 1.1: Average # of passengers by month
        /*JavaPairRDD<String, Integer> passengers_by_month = by_month
                .mapToPair(integerRowTuple2 ->
                        new Tuple2<>(
                                integerRowTuple2._1().substring(5,7),
                                (int) integerRowTuple2._2().get(columns.get("passenger_count"))
                        )
                )
                .reduceByKey(Integer::sum);
        System.out.println("---------------passenger by month--------------------------");
        Map<String, Integer> counts = passengers_by_month.collectAsMap();
        for (String k: counts.keySet()){
            System.out.println("key" + k +": "+counts.get(k));
        }*/


        // Query 1.2: Ratio

        JavaPairRDD<String, Row> valid = by_month
                .filter(stringRowTuple2 ->
                        ((double)stringRowTuple2._2.get(columns.get("total_amount")) - (double)stringRowTuple2._2.get(columns.get("tolls_amount")) !=0));
        valid.take(10).forEach(System.out::println);


        Map<String, Double> sum_ratio_by_month = valid
                .mapToPair(stringRowTuple2 ->
                        new Tuple2<>(
                                stringRowTuple2._1().substring(5,7),
                                (double) stringRowTuple2._2().get(columns.get("tip_amount")) /
                                ((double)stringRowTuple2._2().get(columns.get("total_amount")) - (double)stringRowTuple2._2().get(columns.get("tolls_amount")))
                        )
                )
                .reduceByKey(Double::sum)
                .collectAsMap();


        Map<String, Integer> denominator = valid
                        .mapToPair(stringRowTuple2 ->
                        new Tuple2<>(
                                stringRowTuple2._1().substring(5,7),
                                1)
                )
                .reduceByKey(Integer::sum)
                .collectAsMap();

    }


}
