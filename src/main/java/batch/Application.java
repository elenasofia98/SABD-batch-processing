package batch;

import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Application {
    protected SparkSession ss;
    protected String dataSource;

    public Application(SparkSession ss, String dataSource){
        this.ss = ss;
        this.dataSource = dataSource;
    }

    private Dataset<Row> load(ArrayList<String> locations) throws Exception {
        Dataset<Row> dataset = null;
        for(String path: locations) {
            Dataset<Row> temp = this.ss.read().parquet(path);

            if(dataset != null){
                if(Arrays.equals(dataset.columns(), temp.columns()))
                    dataset = dataset.union(temp);
                else {
                    throw new Exception("Incompatible columns");
                }
            }
            else
                dataset = temp;
        }

        return dataset;
    }

    private Dataset<Row> preprocessing(ArrayList<String> locations, ArrayList<String> usedColums) throws Exception {
        //Check dataset
        Dataset<Row> dataset = this.load(locations);
        for(String c: dataset.columns()){
            if(!usedColums.contains(c))
                dataset = dataset.drop(c);
        }

        return dataset;

    }

    public void test(ArrayList<String> usedColumns) throws Exception {
        ArrayList<String> locations = new ArrayList<>();
        locations.add("data/yellow_tripdata_2021-12.parquet");
        locations.add("data/yellow_tripdata_2022-01.parquet");
        locations.add("data/yellow_tripdata_2022-02.parquet");

        Dataset<Row> dataset = this.preprocessing(locations, usedColumns);
        for (String c: dataset.columns()){
            System.out.println(c);
        }

    }

    public static Application init(){
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("TLC-batch-processing");

        SparkSession ss = SparkSession.builder().config(conf).getOrCreate();

        String dataSource = "data";
        return new Application(ss, dataSource);
    }
}
