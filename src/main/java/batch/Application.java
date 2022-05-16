package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Application {
    protected JavaSparkContext sc;
    protected String dataSource;

    public Application(JavaSparkContext sc, String dataSource){
        this.sc = sc;
        this.dataSource = dataSource;
    }

    private JavaRDD<String> preprocessing(){
        return this.sc.parallelize(Arrays.asList(
                "if you prick us do we not bleed",
                "if you tickle us do we not laugh",
                "if you poison us do we not die and",
                "if you wrong us shall we not revenge"
        ));

    }

    public void test(){
        JavaRDD<String> lines = this.preprocessing();
        List<String> list = lines.collect();

        for(String line: list){
            System.out.println(line);
        }
    }

    public static Application init(){
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("TLC-batch-processing");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        String dataSource = "local";
        return new Application(sc, dataSource);

    }
}
