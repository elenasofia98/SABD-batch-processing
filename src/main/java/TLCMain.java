import batch.Application;

import java.util.ArrayList;

public class TLCMain {

    public static void main(String[] args) {
        Application app = Application.init();
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

        try {
            app.query1(usedColumns);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}