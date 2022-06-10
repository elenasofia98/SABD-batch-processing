package batch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;

public class TaxiRoute  implements Serializable {
    //public int passenger_count;
    public double tip_amount;
    public double total_amount;
    public double tolls_amount;
    public long payment_type;
    public String tpep_dropoff_datetime;

    public String tpep_pickup_datetime;
    public long PULocationID;

    public String getTpep_pickup_datetime() {
        return tpep_pickup_datetime;
    }

    public void setTpep_pickup_datetime(String tpep_pickup_datetime) {
        this.tpep_pickup_datetime = tpep_pickup_datetime;
    }

    public long getPULocationID() {
        return PULocationID;
    }

    public void setPULocationID(long PULocationID) {
        this.PULocationID = PULocationID;
    }

    public TaxiRoute(){}

    public TaxiRoute(double tip_amount, double total_amount, double tolls_amount, long payment_type,
                     String tpep_dropoff_datetime, String tpep_pickup_datetime, long PULocationID) {
        this.payment_type = payment_type;

        this.tip_amount = tip_amount;
        this.tolls_amount = tolls_amount;
        this.total_amount = total_amount;

        this.tpep_dropoff_datetime = tpep_dropoff_datetime;
        this.tpep_pickup_datetime = tpep_pickup_datetime;

        this.PULocationID = PULocationID;
    }

    @Override
    public String toString() {
        return "TaxiRoute{" +
                "tip_amount=" + tip_amount +
                ", total_amount=" + total_amount +
                ", tolls_amount=" + tolls_amount +
                ", payment_type=" + payment_type +
                ", tpep_dropoff_datetime='" + tpep_dropoff_datetime + '\'' +
                ", tpep_pickup_datetime='" + tpep_pickup_datetime + '\'' +
                ", PULocationID=" + PULocationID +
                '}';
    }

    public LinkedList<String> getAllHours(){
        LinkedList<String> hours =  new LinkedList<>();
        int i;

        int start = Integer.parseInt((this.tpep_pickup_datetime.substring(11, 13)));
        int end = Integer.parseInt(this.tpep_dropoff_datetime.substring(11, 13));
        String date = this.tpep_pickup_datetime;

        if(start > end){
            while (start < 24){
                if(start < 10)
                    hours.add(date.split(" ")[0] + " 0" + start);
                else
                    hours.add(date.split(" ")[0] + " " +start);

                start +=1;
            }
            date = tpep_dropoff_datetime;
        }

        while (start <= end){
            if(start < 10)
                hours.add(date.split(" ")[0] + " 0" + start);
            else
                hours.add(date.split(" ")[0] + " " +start);

            start +=1;
        }
        return hours;
    }

    public double getTip_amount() {
        return tip_amount;
    }

    public void setTip_amount(double tip_amount) {
        this.tip_amount = tip_amount;
    }

    public double getTotal_amount() {
        return total_amount;
    }

    public void setTotal_amount(double total_amount) {
        this.total_amount = total_amount;
    }

    public double getTolls_amount() {
        return tolls_amount;
    }

    public void setTolls_amount(double tolls_amount) {
        this.tolls_amount = tolls_amount;
    }

    public long getPayment_type() {
        return payment_type;
    }

    public void setPayment_type(long payment_type) {
        this.payment_type = payment_type;
    }

    public String getTpep_dropoff_datetime() {
        return tpep_dropoff_datetime;
    }

    public void setTpep_dropoff_datetime(String date) {
        this.tpep_dropoff_datetime = date;
    }
}
