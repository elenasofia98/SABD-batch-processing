package batch;

import java.io.Serializable;
import java.util.LinkedList;

public class TaxiRoute  implements Serializable {
    //public int passenger_count;
    public double tipAmount;
    public double totalAmount;
    public double tollsAmount;
    public long paymentType;
    public String tpepDropoffDatetime;

    public String tpepPickupDatetime;
    public long PULocationID;

    public String getTpepPickupDatetime() {
        return tpepPickupDatetime;
    }

    public void setTpepPickupDatetime(String tpepPickupDatetime) {
        this.tpepPickupDatetime = tpepPickupDatetime;
    }

    public long getPULocationID() {
        return PULocationID;
    }

    public void setPULocationID(long PULocationID) {
        this.PULocationID = PULocationID;
    }

    public TaxiRoute(){}

    public static TaxiRoute deserialize(String csvString){
        String[] splitcsv = csvString.split(",");
        return new TaxiRoute(Double.parseDouble(splitcsv[0]), Double.parseDouble(splitcsv[1]), Double.parseDouble(splitcsv[2]),
                Long.parseLong(splitcsv[3]), splitcsv[4], splitcsv[5], Long.parseLong(splitcsv[6]));
    }

    public TaxiRoute(double tipAmount, double totalAmount, double tollsAmount, long paymentType,
                     String tpepDropoffDatetime, String tpepPickupDatetime, long PULocationID) {
        this.paymentType = paymentType;

        this.tipAmount = tipAmount;
        this.tollsAmount = tollsAmount;
        this.totalAmount = totalAmount;

        this.tpepDropoffDatetime = tpepDropoffDatetime;
        this.tpepPickupDatetime = tpepPickupDatetime;

        this.PULocationID = PULocationID;
    }

    @Override
    public String toString() {
        return "TaxiRoute{" +
                "tip_amount=" + tipAmount +
                ", total_amount=" + totalAmount +
                ", tolls_amount=" + tollsAmount +
                ", payment_type=" + paymentType +
                ", tpep_dropoff_datetime='" + tpepDropoffDatetime + '\'' +
                ", tpep_pickup_datetime='" + tpepPickupDatetime + '\'' +
                ", PULocationID=" + PULocationID +
                '}';
    }

    public LinkedList<String> getAllHours(){
        LinkedList<String> hours =  new LinkedList<>();
        int i;

        int start = Integer.parseInt((this.tpepPickupDatetime.substring(11, 13)));
        int end = Integer.parseInt(this.tpepDropoffDatetime.substring(11, 13));
        String date = this.tpepPickupDatetime;

        if(start > end){
            while (start < 24){
                if(start < 10)
                    hours.add(date.split(" ")[0] + " 0" + start);
                else
                    hours.add(date.split(" ")[0] + " " +start);

                start +=1;
            }
            date = tpepDropoffDatetime;
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

    public double getTipAmount() {
        return tipAmount;
    }

    public void setTipAmount(double tipAmount) {
        this.tipAmount = tipAmount;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public double getTollsAmount() {
        return tollsAmount;
    }

    public void setTollsAmount(double tollsAmount) {
        this.tollsAmount = tollsAmount;
    }

    public long getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(long paymentType) {
        this.paymentType = paymentType;
    }

    public String getTpepDropoffDatetime() {
        return tpepDropoffDatetime;
    }

    public void setTpepDropoffDatetime(String date) {
        this.tpepDropoffDatetime = date;
    }
}
