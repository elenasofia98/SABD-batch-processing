package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

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

    protected static final Comparator<String> comparator = new Query2Comparator();

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
                .withColumn("tpep_dropoff_datetime",
                        date_format(dataset.col("tpep_dropoff_datetime"), "yyyy/MM/dd hh:mm"))
                .filter(col("tpep_dropoff_datetime").gt(lit("2021/11/31"))
                        .and(col("tpep_dropoff_datetime").lt(lit("2022/03/01"))))
                .withColumn("tpep_pickup_datetime",
                        date_format(dataset.col("tpep_pickup_datetime"), "yyyy/MM/dd hh:mm"))
                .filter(col("tpep_pickup_datetime").gt(lit("2021/11/31"))
                        .and(col("tpep_pickup_datetime").lt(lit("2022/03/01"))));

        // tip_amount(double)| tolls_amount(double)|total_amount(double) |month (int)
        dataset = dataset
                .withColumn("payment_type", dataset.col("payment_type").cast("long"))
                //.withColumn("passenger_count", dataset.col("passenger_count").cast("int"))
                .withColumn("tip_amount", dataset.col("tip_amount").cast("double"))
                .withColumn("tolls_amount", dataset.col("tolls_amount").cast("double"))
                .withColumn("total_amount", dataset.col("total_amount").cast("double"))
                .withColumn("PULocationID", dataset.col("PULocationID").cast("long"));

        return dataset.as(Encoders.bean(TaxiRoute.class));
    }



    public void query1(JavaRDD<TaxiRoute> rdd){
        // Query 1
        // Ratio
        JavaPairRDD<String, Double> valid = rdd
                .filter(route -> route.payment_type == 1 && route.total_amount != 0)

                .mapToPair(route -> new Tuple2<>(
                        route.tpep_dropoff_datetime.substring(5,7),
                        new Tuple2<>(
                                route.tip_amount / (route.total_amount - route.tolls_amount),
                                1
                        )
                ))

                .reduceByKey((doubleIntegerTuple2, doubleIntegerTuple22) -> new Tuple2<>(
                        Double.sum(doubleIntegerTuple2._1, doubleIntegerTuple22._1),
                        Integer.sum(doubleIntegerTuple2._2, doubleIntegerTuple22._2)
                ))

                .mapValues(doubleIntegerTuple2 -> (double) doubleIntegerTuple2._1 / doubleIntegerTuple2._2);

        Map<String, Double> ratio_by_month = valid
                .collectAsMap();

        for (String k: ratio_by_month.keySet()){
            System.out.println("------>key " + k +": "+ratio_by_month.get(k));
        }

        //TODO
        // save to HDFS


    }

    private JavaPairRDD<String, List<Double>> sub1query2(JavaPairRDD<String, TaxiRoute> base){
        return base
                .mapToPair(routeTuple2 -> new Tuple2<>(
                        routeTuple2._1 //.replace('/', '-').replace(' ', '-')
                                + "," + routeTuple2._2.PULocationID,
                        1))
                .reduceByKey(Integer::sum)
                .mapToPair(stringIntegerTuple2 -> {
                    String[] keys = stringIntegerTuple2._1.split(",");

                    return new Tuple2<String, Tuple2<Long, Integer>>(
                            keys[0],
                            new Tuple2<Long, Integer>(
                                    Long.valueOf(keys[1]),
                                    stringIntegerTuple2._2
                            )
                    );
                })
                .groupByKey()
                .mapValues(tuples -> {
                    List<Double> result = new ArrayList<>();
                    for(int i = 0; i<265; i++){
                        result.add(0d);
                    }

                    int n = 0;
                    Integer count;
                    for (Tuple2<Long, Integer> tuple : tuples) {
                        count = tuple._2;

                        result.add( tuple._1.intValue() - 1, count.doubleValue());
                        n += count;
                    }

                    for(int i = 0; i< result.size(); i ++){
                        result.set(i, result.get(i)/n);
                    }

                    return result;
                });
    }

    private JavaPairRDD<String, Tuple2<Double, Double>> sub2query2(JavaPairRDD<String, TaxiRoute> base){
        JavaPairRDD<String, Tuple3<Double, Double, Integer>> tip_by_hour = base
                .mapValues(route -> new Tuple3<>(
                        Math.pow(route.tip_amount, 2),
                        route.tip_amount,
                        1)
                );

        tip_by_hour = tip_by_hour.cache();


        JavaPairRDD<String, Tuple2<Double, Double>> mean_tip_by_hour = tip_by_hour
                .reduceByKey((doubleIntegerTuple, doubleIntegerTuple2) -> new Tuple3<>(
                        Double.sum(doubleIntegerTuple._1(), doubleIntegerTuple2._1()),
                        Double.sum(doubleIntegerTuple._2(), doubleIntegerTuple2._2()),
                        Integer.sum(doubleIntegerTuple._3(), doubleIntegerTuple2._3())
                ))
                .mapValues(doubleIntegerTuple -> new Tuple2<Double, Double>(
                        doubleIntegerTuple._2() / doubleIntegerTuple._3(),
                        Math.sqrt( ((double) (1/doubleIntegerTuple._3()) * doubleIntegerTuple._1()) - Math.pow(doubleIntegerTuple._2() / doubleIntegerTuple._3(), 2))
                        ));

        return mean_tip_by_hour;
    }


    private JavaPairRDD<String, Long> sub3query2(JavaPairRDD<String, TaxiRoute> base){
        return base.mapToPair(routeTuple2 -> new Tuple2<>(
                        routeTuple2._1 + ',' + routeTuple2._2.payment_type,
                        1
                ))
                .reduceByKey(Integer::sum)
                .mapToPair(stringIntegerTuple2 -> {
                    String[] keys  = stringIntegerTuple2._1.split(",");
                    return new Tuple2<>(
                            keys[0],
                            new Tuple2<Long, Integer>(
                                    Long.valueOf(keys[1]),
                                    stringIntegerTuple2._2
                            )
                    );
                })
                .reduceByKey((stringIntegerTuple1, stringIntegerTuple2) -> {
                    if(stringIntegerTuple1._2 > stringIntegerTuple2._2){
                        return stringIntegerTuple1;
                    }
                    else {
                        return stringIntegerTuple2;
                    }
                })
                .mapValues(stringIntegerTuple2 -> stringIntegerTuple2._1);
    }

    public void query2(JavaRDD<TaxiRoute> rdd) {
        JavaPairRDD<String, TaxiRoute> base = JavaPairRDD.fromJavaRDD(rdd.flatMap(route -> {
            List<Tuple2<String, TaxiRoute>> segments = new LinkedList<>();
            for (String i : route.getAllHours()) {
                segments.add(new Tuple2<>(
                        i, route
                ));
            }
            return segments.iterator();
        }));
        base = base.cache();
        base.take(10).forEach(System.out::println);

        // 2.1
        // distribution over PULocation hour by hour
        JavaPairRDD<String, List<Double>> distribution = this.sub1query2(base);
        distribution.take(1).forEach(System.out::println);


        // 2.2
        // Average and standard deviation tip
        JavaPairRDD<String, Tuple2<Double, Double>> tip = this.sub2query2(base);
        tip.take(10).forEach(System.out::println);


        // 2.3
        // Preferred payment type
        JavaPairRDD<String, Long> payment_type = this.sub3query2(base);
        payment_type.take(10).forEach(System.out::println);





    }


    public void close(){
        this.ss.close();
    }




}
