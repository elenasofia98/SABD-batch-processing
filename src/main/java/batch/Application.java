package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
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

        //System.out.println("------>total " + dataset.count());
        dataset = dataset
                .filter((FilterFunction<Row>) row -> ! row.anyNull());
        //System.out.println("------>not null " + dataset.count());

        dataset = dataset
                .withColumn("date",
                        date_format(dataset.col("tpep_dropoff_datetime"), "yyyy/MM/dd"))
                .filter(col("date").gt(lit("2021/11/30")).and(col("date").lt(lit("2022/03/01"))))
                .drop("tpep_dropoff_datetime");

        // tip_amount(double)| tolls_amount(double)|total_amount(double) |month (int)
        dataset = dataset
                .withColumn("passenger_count", dataset.col("passenger_count").cast("int"))
                .withColumn("tip_amount", dataset.col("tip_amount").cast("double"))
                .withColumn("tolls_amount", dataset.col("tolls_amount").cast("double"))
                .withColumn("total_amount", dataset.col("total_amount").cast("double"));

        return dataset;
    }



    public void query1(JavaRDD<Row> rdd, Hashtable<String, Integer> columns){
        // Query 1: Averages on monthly basis
        JavaPairRDD<String, Row> by_month  = rdd.mapToPair(row -> new Tuple2<>(
                ((String)row.get(columns.get("date"))).substring(5,7),
                row)
        );
        by_month= by_month.cache();

        // Query 1.1: Average # of passengers by month
        JavaPairRDD<String, Integer> passengers_by_month = by_month
                .mapToPair(integerRowTuple2 ->
                        new Tuple2<>(
                                integerRowTuple2._1(),
                                (int) integerRowTuple2._2().get(columns.get("passenger_count"))
                        )
                );

        Map<String, Integer> counts = passengers_by_month.reduceByKey(Integer::sum).collectAsMap();
        for (String k: counts.keySet()){
            System.out.println("------>key " + k +": "+counts.get(k));
        }

        JavaPairRDD<String, Integer> den_passengers_by_month = by_month
                .mapToPair(integerRowTuple2 ->
                        new Tuple2<>(
                                integerRowTuple2._1(),
                                1
                        )
                );
        //passengers_by_month.take(10).forEach(System.out::println);
        Map<String, Integer> den_counts = den_passengers_by_month.reduceByKey(Integer::sum).collectAsMap();
        for (String k: den_counts.keySet()){
            System.out.println("------>key " + k +": "+den_counts.get(k));
        }



        // Query 1.2: Ratio
        JavaPairRDD<String, Row> valid = by_month
                .filter(stringRowTuple2 ->
                        ((double)stringRowTuple2._2.get(columns.get("total_amount")) - (double)stringRowTuple2._2.get(columns.get("tolls_amount")) !=0));
        valid.take(10).forEach(System.out::println);

        Map<String, Double> sum_ratio_by_month = valid
                .mapToPair(stringRowTuple2 ->
                        new Tuple2<>(
                                stringRowTuple2._1(),
                                (double) stringRowTuple2._2().get(columns.get("tip_amount")) /
                                ((double)stringRowTuple2._2().get(columns.get("total_amount")) - (double)stringRowTuple2._2().get(columns.get("tolls_amount")))
                        )
                )
                .reduceByKey(Double::sum)
                .collectAsMap();

        for (String k: sum_ratio_by_month.keySet()){
            System.out.println("------>key " + k +": "+sum_ratio_by_month.get(k));
        }

        Map<String, Integer> denominator = valid
                        .mapToPair(stringRowTuple2 ->
                        new Tuple2<>(
                                stringRowTuple2._1(),
                                1)
                )
                .reduceByKey(Integer::sum)
                .collectAsMap();

        for (String k: denominator.keySet()){
            System.out.println("------>key " + k +": "+sum_ratio_by_month.get(k));
        }

    }




}
