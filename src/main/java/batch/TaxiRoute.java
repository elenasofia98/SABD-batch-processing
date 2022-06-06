package batch;

import java.io.Serializable;

public class TaxiRoute  implements Serializable {
    //public int passenger_count;
    public double tip_amount;
    public double total_amount;
    public double tolls_amount;
    public long payment_type;
    public String date;

    public TaxiRoute(){}

    public TaxiRoute(double tip_amount, double total_amount, double tolls_amount, long payment_type, String date) {
        this.payment_type = payment_type;

        this.tip_amount = tip_amount;
        this.tolls_amount = tolls_amount;
        this.total_amount = total_amount;

        this.date = date;
    }

    @Override
    public String toString() {
        return "TaxiRoute{" +
                ", tip_amount=" + tip_amount +
                ", total_amount=" + total_amount +
                ", tolls_amount=" + tolls_amount +
                ", payment_type=" + payment_type +
                ", date='" + date + '\'' +
                '}';
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

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
