package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
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

    public Dataset<Row> load_parquet(String[] paths) throws Exception {
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


    public JavaRDD<TaxiRoute> load(String[] paths, ArrayList<String> dimensions) throws Exception {

        Dataset<Row> dataset = this.load_parquet(paths);
        for(String c: dataset.columns()){
            if(!dimensions.contains(c))
                dataset = dataset.drop(c);
        }

        dataset = dataset.filter((FilterFunction<Row>) row -> !row.anyNull());

        dataset = dataset
                .withColumn(
                        "tpep_dropoff_datetime",
                        date_format(dataset.col("tpep_dropoff_datetime"), "yyyy/MM/dd hh:mm")
                )
                .withColumn(
                        "tpep_pickup_datetime",
                        date_format(dataset.col("tpep_pickup_datetime"), "yyyy/MM/dd hh:mm")
                )
                .filter(col("tpep_dropoff_datetime").gt(lit("2021/11/31"))
                        .and(col("tpep_dropoff_datetime").lt(lit("2022/03/01"))))
                .filter(col("tpep_pickup_datetime").gt(lit("2021/11/31"))
                        .and(col("tpep_pickup_datetime").lt(lit("2022/03/01"))));

        dataset = dataset
                .withColumn("payment_type"  , dataset.col("payment_type"  ).cast("long")  )
                .withColumn("tip_amount"    , dataset.col("tip_amount"    ).cast("double"))
                .withColumn("tolls_amount"  , dataset.col("tolls_amount"  ).cast("double"))
                .withColumn("total_amount"  , dataset.col("total_amount"  ).cast("double"))
                .withColumn("PULocationID"  , dataset.col("PULocationID"  ).cast("long")  );

        dataset = dataset.cache();
        // (double tipAmount, double totalAmount, double tollsAmount, long paymentType,
        //                     String tpepDropoffDatetime, String tpepPickupDatetime, long PULocationID)
        // tpep_pickup_datetime|tpep_dropoff_datetime|PULocationID|payment_type|tip_amount|tolls_amount|total_amount
        return  dataset.toJavaRDD().map(row -> new TaxiRoute(
                row.getDouble(4),
                row.getDouble(6),
                row.getDouble(5),
                row.getLong(3),
                row.getString(1),
                row.getString(0),
                row.getLong(2)
        ));
    }


    public void saveHDFS(JavaRDD<String> rdd, String path){
        rdd.saveAsTextFile(this.hdfs+'/'+path);
    }

    public void query1(JavaRDD<TaxiRoute> rdd){
        // Query 1
        // Ratio
        JavaPairRDD<String, Tuple2<Double, Integer>> ratio_by_month = rdd
                .filter(route -> route.paymentType == 1 && route.totalAmount != 0)
                .mapToPair(route -> new Tuple2<>(
                        route.tpepDropoffDatetime.substring(0,7).replace('/', '-'),
                        new Tuple2<>(
                                route.tipAmount / (route.totalAmount - route.tollsAmount),
                                1
                        )
                ))

                .reduceByKey((doubleIntegerTuple, doubleIntegerTuple2) -> new Tuple2<>(
                        Double.sum(doubleIntegerTuple._1, doubleIntegerTuple2._1),
                        Integer.sum(doubleIntegerTuple._2, doubleIntegerTuple2._2)
                ))

                .mapValues(doubleIntegerTuple -> new Tuple2<>(
                        doubleIntegerTuple._1 / doubleIntegerTuple._2,
                        doubleIntegerTuple._2)
                );

        this.saveHDFS(ratio_by_month.map(stringTuple2Tuple2 ->
                stringTuple2Tuple2._1 + "," +
                stringTuple2Tuple2._2._1 +  "," +
                stringTuple2Tuple2._2._2), "query1");
    }

    private JavaPairRDD<String, List<Double>> sub1query2(JavaPairRDD<String, TaxiRoute> base){
        return base
                .mapToPair(routeTuple2 -> new Tuple2<>(
                        routeTuple2._1 + "," + routeTuple2._2.PULocationID,
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
                        Math.pow(route.tipAmount, 2),
                        route.tipAmount,
                        1)
                );

        return tip_by_hour
                .reduceByKey((doubleIntegerTuple, doubleIntegerTuple2) -> new Tuple3<>(
                        Double.sum(doubleIntegerTuple._1(), doubleIntegerTuple2._1()),
                        Double.sum(doubleIntegerTuple._2(), doubleIntegerTuple2._2()),
                        Integer.sum(doubleIntegerTuple._3(), doubleIntegerTuple2._3())
                ))
                .mapValues(doubleIntegerTuple -> new Tuple2<Double, Double>(
                        doubleIntegerTuple._2() / doubleIntegerTuple._3(),
                        Math.sqrt(  ((double) 1/doubleIntegerTuple._3()) * doubleIntegerTuple._1() - Math.pow( ((double)doubleIntegerTuple._2() / doubleIntegerTuple._3()), 2))
                        ));
    }


    private JavaPairRDD<String, Long> sub3query2(JavaPairRDD<String, TaxiRoute> base){
        return base.mapToPair(routeTuple2 -> new Tuple2<>(
                        routeTuple2._1 + ',' + routeTuple2._2.paymentType,
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

        // 2.1
        // distribution over PULocation hour by hour
        JavaPairRDD<String, List<Double>> distribution = this.sub1query2(base);

        // 2.2
        // Average and standard deviation tip
        JavaPairRDD<String, Tuple2<Double, Double>> tip = this.sub2query2(base);

        // 2.3
        // Preferred payment type
        JavaPairRDD<String, Long> payment_type = this.sub3query2(base);


        //Save to HDFS
        this.saveHDFS(
                distribution.join(tip).join(payment_type)
                        .map(stringTuple2Tuple2 -> {
                                String key = stringTuple2Tuple2._1
                                        .replace("/","-")
                                        .replace(" ", "-");

                                List<Double> distr = stringTuple2Tuple2._2._1._1;

                                Double tip_averarage = stringTuple2Tuple2._2._1._2._1;
                                Double tip_std = stringTuple2Tuple2._2._1._2._2;

                                Long payment_tyoe = stringTuple2Tuple2._2._2;

                                String sep = ",";
                                StringBuilder sb = new StringBuilder();
                                sb.append(key);
                                sb.append(sep);
                                for(double d: distr){
                                    sb.append(d);
                                    sb.append(sep);
                                }

                                sb.append(tip_averarage);
                                sb.append(sep);
                                sb.append(tip_std);
                                sb.append(sep);

                                sb.append(payment_tyoe);
                                return sb.toString();

                            }
                        ),
                "query2");

    }


    public void close(){
        this.ss.close();
    }




}
