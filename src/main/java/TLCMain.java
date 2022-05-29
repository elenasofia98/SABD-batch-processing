import batch.Application;

import java.util.ArrayList;

public class TLCMain {

    public static void main(String[] args) {
        Application app = Application.init();
        ArrayList<String> usedColumns = new ArrayList<>();
        usedColumns.add("fare_amount");
        usedColumns.add("extra");
        usedColumns.add("mta_tax");

        //"tip_amount"
        //"tolls_amount"
        //"improvement_surcharge"
        //"total_amount"
        //"congestion_surcharge"
        //"airport_fee"

        try {
            app.test(usedColumns);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}