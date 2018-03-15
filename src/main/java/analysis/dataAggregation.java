package analysis;

import Utility.util;
import saveData.saveDataToDatabase;
import main.MainFunction;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Qijian-Gan on 10/4/2017.
 */
public class dataAggregation {

    // ***********************************************************//
    // This is the main function to aggregate the raw data to longer intervals
    public static void mainAggregationHistorical(){
        if(MainFunction.cBlock.organizationAggregation.equals("LACO")) {
            //Create two workers for Arcadia and LACO
            MyRunnableAggregationArcadia worker1 = new MyRunnableAggregationArcadia();
            Thread t1 = new Thread(worker1);
            t1.start();
            MyRunnableAggregationLACO worker2 = new MyRunnableAggregationLACO();
            Thread t2 = new Thread(worker2);
            t2.start();
        }else if(MainFunction.cBlock.organizationAggregation.equals("Pasadena")){
            aggregatedDataPasadena();
        }
    }

    // Aggregate Arcadia's data
    public static class MyRunnableAggregationArcadia implements Runnable{
        // This is the class for runnable programs
        public void run()
        {
            aggregatedDataArcadia();
        }
    }
    public static void aggregatedDataArcadia(){
        // Create the connections to the databases
        try {
            // Create the connections to the databases
            MainFunction.conArcadia = DriverManager.getConnection(MainFunction.hostIENArcadia,
                    MainFunction.userName, MainFunction.password);
            System.out.println("Aggregating detector data for Arcadia!");
            List<dataAggregation.aggregatedDetectorData> aggregatedData=
                    dataAggregation.aggregationToLongerIntervalHistorical(MainFunction.conArcadia,
                            MainFunction.cBlock.interval,MainFunction.cBlock.fromDateString, MainFunction.cBlock.toDateString);
            System.out.println("Saving aggregated detector data for Arcadia!");
            saveDataToDatabase.insertAggregatedDetectorDataToDataBaseBatch(MainFunction.conArcadia, aggregatedData);
            System.out.println("Done for Arcadia!");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    // Aggregate LACO's data
    public static class MyRunnableAggregationLACO implements Runnable{
        // This is the class for runnable programs
        public void run()
        {
            aggregatedDataLACO();
        }
    }
    public static void aggregatedDataLACO(){
        // Create the connections to the databases
        try {
            // Create the connections to the databases
            MainFunction.conLACO = DriverManager.getConnection(MainFunction.hostIENLACO,
                    MainFunction.userName, MainFunction.password);
            System.out.println("Aggregating detector data for LACO!");
            List<dataAggregation.aggregatedDetectorData> aggregatedData=
                    dataAggregation.aggregationToLongerIntervalHistorical(MainFunction.conLACO, MainFunction.cBlock.interval,
                            MainFunction.cBlock.fromDateString, MainFunction.cBlock.toDateString);
            System.out.println("Saving aggregated detector data for LACO!");
            saveDataToDatabase.insertAggregatedDetectorDataToDataBaseBatch(MainFunction.conLACO, aggregatedData);
            System.out.println("Done for LACO!");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void aggregatedDataPasadena(){
        // Create the connections to the databases
        try {
            // Create the connections to the databases
            MainFunction.conPasadena = DriverManager.getConnection(MainFunction.hostPasadena,
                    MainFunction.userName, MainFunction.password);
            System.out.println("Aggregating detector data for Pasadena!");
            List<dataAggregation.aggregatedDetectorData> aggregatedData=
                    dataAggregation.aggregationToLongerIntervalHistoricalPasadena(MainFunction.conPasadena,
                            MainFunction.cBlock.interval,
                            MainFunction.cBlock.fromDateString, MainFunction.cBlock.toDateString);
            System.out.println("Saving aggregated detector data for Pasadena!");
            saveDataToDatabase.insertAggregatedDetectorDataToDataBaseBatch(MainFunction.conPasadena, aggregatedData);
            System.out.println("Done for Pasadena!");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    //***********Below are the major functions**************
    public static class aggregatedDetectorData{
        // This is the profile for the five-minute detector data
        public aggregatedDetectorData(int _DetectorID, int _Date, int _Time, double _Speed, double _Occupancy, double _Volume){
            this.DetectorID=_DetectorID;
            this.Date=_Date;
            this.Time=_Time;
            this.Speed=_Speed;
            this.Occupancy=_Occupancy;
            this.Volume=_Volume;
        }
        public int DetectorID;
        public int Date;
        public int Time;
        public double Speed;
        public double Occupancy;
        public double Volume;
    }

    public static List<aggregatedDetectorData> aggregationToLongerIntervalHistorical(Connection con, int interval,
                                                                                     String fromDateString, String toDateString){

        // This function is used to aggregated the raw data into longer intervals (Historical or manually)

        // Create the list of aggregated data
        List<aggregatedDetectorData> aggregatedDataList= new ArrayList<aggregatedDetectorData>();
        // Get the number of steps
        int numOfSteps=24*3600/interval;

        try {
            Statement ps=con.createStatement();
            Date fromDate = new SimpleDateFormat("yyyy-MM-dd").parse(fromDateString);
            Date toDate = new SimpleDateFormat("yyyy-MM-dd").parse(toDateString);

            Date tmpDate = fromDate;
            while(tmpDate.compareTo(toDate)<=0){ // Loop for each date
                // Get the integer format of date

                int year=util.dateToNumber(tmpDate, "Year");
                int month=util.dateToNumber(tmpDate, "Month");
                int day=util.dateToNumber(tmpDate, "Day");
                int date=year*10000+month*100+day;

                // Get the detector IDs for a given date
                List<Integer> detectorIDs= new ArrayList<Integer>();
                String sql="select distinct(DetectorID) from detector_raw_data_"+year+ " where Date="+date;
                ResultSet resultSet=ps.executeQuery(sql);
                int detectorID;
                while(resultSet.next()){
                    detectorID=resultSet.getInt("DetectorID");
                    if(detectorID!=9901){
                        detectorIDs.add(detectorID);
                    }
                }

                System.out.println("Num of detectors="+detectorIDs.size()+" & Date="+date);
                for(int i=0;i<detectorIDs.size();i++){ // Loop for each detector ID
                    detectorID=detectorIDs.get(i);
                    // Select the data belonging to the detectorID and date
                    sql="Select Time, Speed, Occupancy, Volume from detector_raw_data_"+year+" where Date="+date+
                            " and DetectorID="+detectorID;
                    resultSet=ps.executeQuery(sql);
                    // Get the detector data for the given date
                    List<double[]> detectorData=util.convertResultsetToArray(resultSet);

                    if(!detectorData.isEmpty()){// If the result set is not null
                        for (int j=0;j<numOfSteps;j++){// Loop for each step
                            //Get the detector data with the jth time step
                            List<double []> tmpDetectorData=new ArrayList<double[]>();
                            int time=j*interval;// Get the current time

                            for (int k=0;k<detectorData.size();k++){
                                double [] tmpData=detectorData.get(k);
                                if(tmpData[0]>=time && tmpData[0]<(time+interval)){
                                    tmpDetectorData.add(tmpData);
                                }
                                if(tmpData[0]>=time+interval){
                                    break;
                                }
                            }
                            if(tmpDetectorData.size()>0) {
                                // Get the aggregated data
                                aggregatedDetectorData aggregatedData = rawToAggregatedData
                                        (detectorID, date, time, tmpDetectorData, interval);
                                // Add it to the list with longer intervals
                                aggregatedDataList.add(aggregatedData);
                            }
                        }// End of each time step
                    }
                }// End of each detector
                tmpDate=util.addDays(tmpDate,1);
            }// End of each date
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return aggregatedDataList;
    }

    public static List<aggregatedDetectorData> aggregationToLongerIntervalHistoricalPasadena(Connection con, int interval,
                                                                                     String fromDateString, String toDateString){

        // This function is used to aggregated the raw data into longer intervals for Pasadena
        // The derived ID for Pasadena is: IntersectionID(3~4 digits)+Detector ID(2 digits)

        // Create the list of aggregated data
        List<aggregatedDetectorData> aggregatedDataList= new ArrayList<aggregatedDetectorData>();
        // Get the number of steps
        int numOfSteps=24*3600/interval;

        try {
            Statement ps=con.createStatement();
            Date fromDate = new SimpleDateFormat("yyyy-MM-dd").parse(fromDateString);
            Date toDate = new SimpleDateFormat("yyyy-MM-dd").parse(toDateString);

            Date tmpDate = fromDate;
            while(tmpDate.compareTo(toDate)<=0){ // Loop for each date
                // Get the integer format of date

                int year=util.dateToNumber(tmpDate, "Year");
                int month=util.dateToNumber(tmpDate, "Month");
                int day=util.dateToNumber(tmpDate, "Day");
                int date=year*10000+month*100+day;

                // Get the detector IDs for a given date
                List<Integer> detectorIDs= new ArrayList<Integer>();
                String sql="select distinct IntersectionID, DetectorID from detector_raw_data_"+year+ " where Date="+date;
                ResultSet resultSet=ps.executeQuery(sql);
                int detectorID;
                while(resultSet.next()){
                    detectorID=resultSet.getInt("IntersectionID")*100+resultSet.getInt("DetectorID"); // The last two digits are for the true detector ID
                    if(detectorID!=9901){
                        detectorIDs.add(detectorID);
                    }
                }

                System.out.println("Num of detectors="+detectorIDs.size()+" & Date="+date);
                for(int i=0;i<detectorIDs.size();i++){ // Loop for each detector ID
                    detectorID=detectorIDs.get(i);
                    // Select the data belonging to the detectorID and date
                    int IntID=detectorID/100;
                    int DetID=detectorID-IntID*100;
                    sql="Select Time, Speed, Occupancy, Volume, Period from detector_raw_data_"+year+" where Date="+date+
                            " and DetectorID="+DetID + " and IntersectionID="+IntID;
                    resultSet=ps.executeQuery(sql);
                    // Get the detector data for the given date
                    List<double[]> detectorData=util.convertResultsetToArray(resultSet);

                    if(!detectorData.isEmpty()){// If the result set is not null
                        for (int j=0;j<numOfSteps;j++){// Loop for each step
                            //Get the detector data with the jth time step
                            List<double []> tmpDetectorData=new ArrayList<double[]>();
                            int time=j*interval;// Get the current time

                            for (int k=0;k<detectorData.size();k++){
                                double [] tmpData=detectorData.get(k);
                                if(tmpData[0]>=time && tmpData[0]<(time+interval)){
                                    tmpDetectorData.add(tmpData);
                                }
                                if(tmpData[0]>=time+interval){
                                    break;
                                }
                            }
                            if(tmpDetectorData.size()>0) {
                                // Get the aggregated data
                                aggregatedDetectorData aggregatedData = rawToAggregatedData
                                        (detectorID, date, time, tmpDetectorData, interval);
                                // Add it to the list with longer intervals
                                aggregatedDataList.add(aggregatedData);
                            }
                        }// End of each time step
                    }
                }// End of each detector
                tmpDate=util.addDays(tmpDate,1);
            }// End of each date
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return aggregatedDataList;
    }


    public static aggregatedDetectorData rawToAggregatedData(int detectorID, int date, int time,
                                                             List<double []> rawData,int interval){
        // This function is used to aggregate raw data to longer intervals
        // rawData: Time, Speed (mph), Occupancy (%), Volume (vph)
        double speed=0;
        double occupancy=0;
        double volume=0;

        if(rawData.isEmpty()){
            return null;
        }else{
            // Duplicated the last row
            double [] tmpData=util.copyValueWithoutPointerDouble(rawData.get(rawData.size()-1));
            // The following sentence will be wrong if I use double [] tmpData=rawData.get(rawData.size()-1);
            // Because it assigns points instead of the values
            tmpData[0]=time+interval;
            rawData.add(tmpData);

            double refTime=time;// Get the reference time
            for (int i=0;i<rawData.size();i++){ // Loop for each row
                tmpData=util.copyValueWithoutPointerDouble(rawData.get(i)); // Get the current row
                volume=volume+tmpData[1]*(tmpData[0]-refTime)*1.0/interval;  // Take the weighted value of volume
                occupancy=occupancy+tmpData[2]*(tmpData[0]-refTime)*1.0/interval; // Take the weighted value of occupancy
                speed=speed+tmpData[3]*(tmpData[0]-refTime)*1.0/interval; // Take the weighted value of speed
                refTime=tmpData[0]; // Update the reference time
            }
            // Create the aggregated data
            aggregatedDetectorData aggregatedData= new aggregatedDetectorData(detectorID,date,time,speed,occupancy,volume);
            return aggregatedData;
        }
    }

}
