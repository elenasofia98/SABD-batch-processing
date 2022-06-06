package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.collection.Seq;

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

    private Dataset<Row> load_parquet(ArrayList<String> paths) throws Exception {
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


    public Dataset<TaxiRoute> load(ArrayList<String> pahts, ArrayList<String> usedColumns) throws Exception {

        Dataset<Row> dataset = this.load_parquet(pahts);
        for(String c: dataset.columns()){
            //System.out.println(c);
            if(!usedColumns.contains(c))
                dataset = dataset.drop(c);
        }

        //System.out.println("------>total " + dataset.count());
        //dataset = dataset.filter((FilterFunction<Row>) row -> ! row.anyNull());
        //System.out.println("------>not null " + dataset.count());

        dataset = dataset
                .withColumn("date",
                        date_format(dataset.col("tpep_dropoff_datetime"), "yyyy/MM/dd"))
                .filter(col("date").gt(lit("2021/11/30")).and(col("date").lt(lit("2022/03/01"))))
                .drop("tpep_dropoff_datetime");

        // tip_amount(double)| tolls_amount(double)|total_amount(double) |month (int)
        dataset = dataset
                .withColumn("payment_type", dataset.col("payment_type").cast("long"))
                //.withColumn("passenger_count", dataset.col("passenger_count").cast("int"))
                .withColumn("tip_amount", dataset.col("tip_amount").cast("double"))
                .withColumn("tolls_amount", dataset.col("tolls_amount").cast("double"))
                .withColumn("total_amount", dataset.col("total_amount").cast("double"));

        return dataset.as(Encoders.bean(TaxiRoute.class));
    }



    public void query1(JavaRDD<TaxiRoute> rdd){
        // Query 1: Averages on monthly basis
        JavaPairRDD<String, TaxiRoute> by_month  = rdd
                .mapToPair(route -> new Tuple2<>(
                        route.date.substring(5,7),
                        route)
        );

        // Query 1
        // Ratio
        JavaPairRDD<String, Double> valid = by_month
                .filter(routeTuple -> routeTuple._2.payment_type == 1)
                .mapValues(route -> route.tip_amount / (route.total_amount - route.tolls_amount))
                .filter(stringDoubleTuple -> !Double.isNaN(stringDoubleTuple._2));
        valid = valid.cache();

        Map<String, Double> sum_ratio_by_month = valid
                .reduceByKey(Double::sum)
                .collectAsMap();

        for (String k: sum_ratio_by_month.keySet()){
            System.out.println("------>key " + k +": "+sum_ratio_by_month.get(k));
        }

        Map<String, Integer> denominator = valid
                .mapValues(row -> 1)
                .reduceByKey(Integer::sum)
                .collectAsMap();

        for (String k: denominator.keySet()){
            System.out.println("------>key " + k +": "+denominator.get(k));
        }


        // save to HDFS

    }




}
