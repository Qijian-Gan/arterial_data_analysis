package analysis;


import Utility.util;
import main.MainFunction;
import saveData.saveDataToDatabase;
import sun.applet.Main;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by Qijian-Gan on 9/21/2017.
 */
public class healthAnalysis {

    public static void mainHealthAnalysisAndDataFiltering(String host,String curOrganization){
        // This function is to do data filtering: fromDate to toDate
        try {
            // Get the connection
            Connection con = DriverManager.getConnection(host, MainFunction.cBlock.userName, MainFunction.cBlock.password);
            System.out.println("Succefully connect to the database!");
            // Get the thresholds
            HealthThreshold healthThreshold=null;
            String organization=null;
            if(MainFunction.cBlock.dataSourceHealth.equals("TCSServer")){
                healthThreshold= new HealthThreshold(MainFunction.cBlock.missingRateThreshold_TCSServer,
                        MainFunction.cBlock.maxZeroValueThreshold_TCSServer,
                        MainFunction.cBlock.highValueRateThreshold_TCSServer,
                        MainFunction.cBlock.highFlowValue_TCSServer,
                        MainFunction.cBlock.inconsisRateWithoutSpeedThreshold_TCSServer);
                organization=curOrganization;
            }else if(MainFunction.cBlock.dataSourceHealth.equals("IEN")||
                    MainFunction.cBlock.dataSourceHealth.equals("Pasadena")){
                healthThreshold= new HealthThreshold(MainFunction.cBlock.missingRateThreshold_IEN,
                        MainFunction.cBlock.maxZeroValueThreshold_IEN,
                        MainFunction.cBlock.highValueRateThreshold_IEN,
                        MainFunction.cBlock.highFlowValue_IEN,
                        MainFunction.cBlock.inconsisRateWithoutSpeedThreshold_IEN);
                organization=curOrganization;
            }else{
                System.out.println("Unkonwn data source!");
                System.exit(-1);
            }
            // Get the date ranges
            Date fromDate = new SimpleDateFormat("yyyy-MM-dd").parse(MainFunction.cBlock.fromDate);
            Date toDate = new SimpleDateFormat("yyyy-MM-dd").parse(MainFunction.cBlock.toDate);
            int numDay = (int) Math.round((toDate.getTime() - fromDate.getTime()) / 1000.0 / 60.0 / 60.0 / 24.0)+1;

            // Loop for each day
            Date tmpDate = fromDate;
            for (int i = 0; i < numDay; i++) {
                System.out.println(tmpDate);
                long start = System.currentTimeMillis();
                // Get the year,month,day for a given date
                int dateNum = util.dateToNumber(tmpDate,"DateNum");
                int year=util.dateToNumber(tmpDate,"Year");
                int month=util.dateToNumber(tmpDate,"Month");
                int day=util.dateToNumber(tmpDate,"Day");

                // First get the number of detectors
                Statement ps = con.createStatement();
                String sql=null;
                if(MainFunction.cBlock.dataSourceHealth.equals("TCSServer")) {
                    sql = "select DISTINCT(DetectorID) from detector_data_raw_" + year + " where Date=" + dateNum + ";";
                }else if(MainFunction.cBlock.dataSourceHealth.equals("IEN") ||
                        MainFunction.cBlock.dataSourceHealth.equals("Pasadena")){
                    sql = "select DISTINCT(DetectorID) from detector_aggregated_data_" + year + " where Date=" + dateNum + ";";
                }
                ResultSet result = ps.executeQuery(sql);
                while (result.next()) {
                    int detectorID = result.getInt("DetectorID");
                    if (detectorID != 9901) {
                        //System.out.println(tmpDate+" and Detector="+detectorID);
                        // Get the raw data
                        String sqlDate=null;
                        if(MainFunction.cBlock.dataSourceHealth.equals("TCSServer")) {
                            sqlDate = "select Time,Volume,Occupancy,Speed from detector_data_raw_" + year + " where (Date="
                                    + dateNum + " and DetectorID=" + detectorID + ") order by Time;";
                        }else if(MainFunction.cBlock.dataSourceHealth.equals("IEN")||
                                MainFunction.cBlock.dataSourceHealth.equals("Pasadena")){
                            sqlDate = "select Time,Volume,Occupancy,Speed from detector_aggregated_data_" + year + " where (Date="
                                    + dateNum + " and DetectorID=" + detectorID + ") order by Time;";
                        }
                        Statement psData = con.createStatement();
                        ResultSet resultData = psData.executeQuery(sqlDate);
                        List<double[]> dataInputArray = util.convertResultsetToArray(resultData);
                        double[][] dataInput = util.convertArrayToMatrix(dataInputArray);

                        // Detector health analysis
                        mainHealthAnalysis(con, organization, detectorID, MainFunction.cBlock.defaultInterval, year, month, day, healthThreshold, dataInput);
                        // Data filtering
                        dataFiltering.mainDataFiltering(con, dataInput, detectorID, year, month, day, MainFunction.cBlock.defaultInterval);
                    }
                }
                long end = System.currentTimeMillis();
                System.out.println("Total time taken = " + (end - start) + " ms");
                tmpDate=util.addDays(tmpDate,1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public static void mainHealthAnalysis(Connection con, String organization,int detectorID, int interval,int year, int month,int day, HealthThreshold healthThreshold,
                                          double [][] dataInput){

        try{
            // Create the statement
            Statement psDetInv = con.createStatement();
            // Get the number of lanes for the given detector
            String sqlNumLane=null;
            if(organization.equals("Arcadia")) {
                // Get the intersection and detector ID
                int intersectionID = detectorID / 100;
                int sensorID = detectorID % 100;
                sqlNumLane = "select NumOfLanes from detector_inventory where (IntersectionID="
                        + intersectionID + " and SensorID=" + sensorID + ");";
            }else if(organization.equals("LACO")){
                sqlNumLane = "select NumOfLanes from detector_inventory where SensorID=\"" + detectorID + "\";";
            }else if(organization.equals("Pasadena")) {
                // Get the intersection and detector ID
                int intersectionID = detectorID / 100;
                int sensorID = detectorID % 100;
                sqlNumLane = "select distinct Lane from detector_inventory where (IntersectionID="
                        + intersectionID + " and DetectorID=" + sensorID + ");";
            }else
            {
                System.out.println("Unknown organization");
                System.exit(-1);
            }
            ResultSet resultLane = psDetInv.executeQuery(sqlNumLane);
            int numOfLane = 1;
            while (resultLane.next()) {
                if(organization.equals("Pasadena")){
                    // Extract lane information
                    String laneStr=resultLane.getString("Lane");
                    numOfLane=0;
                    if(laneStr.contains("1"))
                        numOfLane=numOfLane+1;
                    if(laneStr.contains("2"))
                        numOfLane=numOfLane+1;
                    if(laneStr.contains("3"))
                        numOfLane=numOfLane+1;
                    if(laneStr.contains("4"))
                        numOfLane=numOfLane+1;
                    if(laneStr.contains("5"))
                        numOfLane=numOfLane+1;
                    if(laneStr.contains("6"))
                        numOfLane=numOfLane+1;
                    numOfLane=Math.max(1,numOfLane);
                }else{
                    numOfLane = resultLane.getInt("NumOfLanes");
                }
            }

            // Perform the detector health analysis
            DetectorHealthMetrics detectorHealthMetrics=
                    checkDetectorHealthByDay(interval, dataInput,healthThreshold,numOfLane);
            // Save the health analysis to the database
            saveDataToDatabase.insertHealthMeasurementToDataBase(con, detectorHealthMetrics,detectorID,year,month,day);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static class DetectorHealthMetrics{
        // This function is to define the health metrics for detectors

        public DetectorHealthMetrics(double _MissingRate, double _MaxZeroValue, double _HighValueRate, int _ConstantOrNot,
                                     double _InconsisRateWithoutSpeed, int _Health){
            this.MissingRate=_MissingRate; // Missing rate
            this.MaxZeroValue=_MaxZeroValue; // Max length of zero values
            this.HighValueRate=_HighValueRate; // High value rate
            this.ConstantOrNot=_ConstantOrNot; // Whether it is constant or not
            this.InconsisRateWithoutSpeed=_InconsisRateWithoutSpeed; // Inconsistency rate between flow and occupancy
            this.Health=_Health; // Health: good or bad
        }
        public double MissingRate;
        public double MaxZeroValue;
        public double HighValueRate;
        public int ConstantOrNot;
        public double InconsisRateWithoutSpeed;
        public int Health;
    }

    public static class HealthThreshold{
        // This function is used to set the health thresholds

        public HealthThreshold(double _MissingRateThreshold, double _MaxZeroValuesThreshold,
                               double _HighValueRateThreshold, double _HighFlowValue, double _InconsisRateWithoutSpeedThreshold){
            this.MissingRateThreshold=_MissingRateThreshold; // Threshold for missing rate
            this.MaxZeroValuesThreshold=_MaxZeroValuesThreshold; // Threshold for max length of zero values
            this.HighValueRateThreshold=_HighValueRateThreshold; // Threshold for high value rate
            this.HighFlowValue=_HighFlowValue; // The flow threshold to say a data sample is "high value"
            this.InconsisRateWithoutSpeedThreshold=_InconsisRateWithoutSpeedThreshold; // Threshold for inconsistency rate
        }

        double HighFlowValue;

        double MissingRateThreshold;
        double MaxZeroValuesThreshold;
        double HighValueRateThreshold;
        double InconsisRateWithoutSpeedThreshold;
    }

    public static DetectorHealthMetrics checkDetectorHealthByDay(int interval, double [][] measurements,
                                                HealthThreshold healthThreshold, int numOfLane){
        // This function is used to check the detector health by day
        // Inputs: interval for the data points, e.g., 5min (300sec).
        // time: time period for the raw data.
        // measurements: flow, occupancy, speed with the same length of "time"
        // healthThreshold: predefined health threshold
        // numOfLane: number of lanes for the given detector

        // Check missing rate
        double missingRate=checkMissingRate(interval,measurements);

        // Check high value rate
        double highValueRate=checkHighValueRate(interval, healthThreshold.HighFlowValue, numOfLane, measurements);

        // Check constant or not
        int constantOrNot=checkConstantOrNot(measurements);

        // Check max length of zero values
        double maxLengthZeroValues=checkMaxLengthZeroValues(interval,measurements);

        // Check inconsistency rate (between flow and occupancy
        double inconsisRate=checkInconsisRateWithoutSpeed(interval, measurements);

        // Determine whether a detector is good or bad
        int health=determineGoodOrBad(healthThreshold, missingRate,highValueRate,constantOrNot,maxLengthZeroValues,inconsisRate);

        // Update and return the health metrics
        DetectorHealthMetrics healthMetricsList= new DetectorHealthMetrics(missingRate,maxLengthZeroValues,highValueRate, constantOrNot,
                inconsisRate, health);
        return healthMetricsList;

    }

    public static int determineGoodOrBad(HealthThreshold healthThreshold, double missingRate, double highValueRate,
                                         double constantOrNot, double maxLengthZeroValues, double inconsisRate){
        // This function is used to determine whether a detector is good or bad

        int health=1;
        if(missingRate>healthThreshold.MissingRateThreshold || // High missing rate
                highValueRate>healthThreshold.HighValueRateThreshold|| // High rate of high values
                constantOrNot==1 || // Constant values
                maxLengthZeroValues>healthThreshold.MaxZeroValuesThreshold|| // Long period with zero values
                inconsisRate>healthThreshold.InconsisRateWithoutSpeedThreshold){ // High inconsistency rate between flow and occupancy
            health=0; // Set to bad
        }
        return health;
    }

    public static double checkInconsisRateWithoutSpeed(int interval, double [][] measurements){
        // This function is used to check the inconsistency rate between flow and occupancy

        // measurements: time, flow, occupancy, speed
        int numOfInterval=24*3600/interval; // Get the number of intervals;

        int numOfInconsisData=0;
        for (int i=0;i<measurements.length;i++){
            if(measurements[i][1]>0 && measurements[i][2]==0){ // Flow>0 but Occ=0
                numOfInconsisData=numOfInconsisData+1;
            }
        }

        double inconsisRate=numOfInconsisData*1.0/numOfInterval*100;
        return inconsisRate;
    }


    public static double checkMaxLengthZeroValues(int interval, double [][] measurements){
        // This function is used to check the maximum length of zero values

        // Note: the input measurements should be "rows" with unique times
        // measurements: time, flow, occupancy, speed
        double maxLengthZeroValues=0;

        //Note: we only focus on the time period from 6AM to 10PM since some minor streets may have zero flows during midnight
        int startTime=6*3600;
        int endTime=22*3600;

        double curZeroLength=0; // Set the current zero-length to be zero
        for (int i=0;i<measurements.length;i++){ // Loop for each row
            if(measurements[i][0]>=startTime && measurements[i][0]<endTime){ // If it is within the defined time duration

                if(measurements[i][1]==0 && measurements[i][2]==0 && measurements[i][3]==0){ // All zeros: flow, occ, and speed
                    curZeroLength=curZeroLength+1; // Add one
                }else{
                    if(curZeroLength>=maxLengthZeroValues){ // Assign the value to maxLengthZeroValues
                        maxLengthZeroValues=curZeroLength;
                    }
                    curZeroLength=0; //Reset to zero
                }
            }
        }

        // If is possible that the values are all zero during the selected time period
        if(curZeroLength>=maxLengthZeroValues){
            maxLengthZeroValues=curZeroLength;
        }

        double maxLengthZeroValuesByHour=maxLengthZeroValues*interval*1.0/3600;
        return maxLengthZeroValuesByHour;
    }

    public static int checkConstantOrNot(double [][] measurements){
        // THis function is used to check the input measurements are constant or not

        // Note: the input measurements should be "rows" with unique times
        // measurements: time, flow, occupancy, speed
        int constantOrNot=0;

        // Get the means of flow and occupancy
        double meanFlow=0;
        double meanOcc=0;
        for (int i=0;i<measurements.length;i++){
            meanFlow=meanFlow+measurements[i][1]*1.0/measurements.length;
            meanOcc=meanOcc+measurements[i][2]*1.0/measurements.length;
        }

        // Get the std of flow and occupancy
        double devFlow=0;
        double devOcc=0;
        for (int i=0;i<measurements.length;i++){
            devFlow=devFlow+(measurements[i][1]-meanFlow)*(measurements[i][1]-meanFlow)*1.0/(measurements.length-1);
            devOcc=devOcc+(measurements[i][2]-meanOcc)*(measurements[i][2]-meanOcc)*1.0/(measurements.length-1);
        }
        double stdFlow=Math.sqrt(devFlow);
        double stdOcc=Math.sqrt(devOcc);

        // Check whether it is constant or not
        if((meanFlow>=0 && stdFlow==0)||(meanOcc>=0 && stdOcc==0)){ // if one of measurements is constant
            if(meanFlow+meanOcc>0){ // If they are not all empty
                constantOrNot=1;
            }
        }
        return constantOrNot;

    }

    public static double checkHighValueRate(int interval, double flowThreshold, int numOfLane,double[][] measurements){
        // This function is used to check the high value rate

        // Note: the input measurements should be "rows" with unique times

        // measurements: time, flow, occupancy, speed
        int numOfInterval=24*3600/interval; // Get the number of intervals;

        // Get the number of high values
        int curHighValueNumber=0;
        for (int i=0;i<measurements.length; i++){
            if(measurements[i][1]>flowThreshold*numOfLane){
                curHighValueNumber=curHighValueNumber+1;
            }
        }

        double highValueRate=curHighValueNumber*100.0/numOfInterval;
        return highValueRate;
    }


    public static double checkMissingRate(int interval, double [][] measurements){
        // This function is used to check the daily data missing rate

        // Note: the input measurements should be "rows" with unique times
        // measurements: time, flow, occupancy, speed
        int numOfInterval=24*3600/interval; // Get the number of intervals;
        int curNumOfInterval=measurements.length; // Get the current number of intervals

        double missingRate= Math.max(0,(numOfInterval-curNumOfInterval)*100.0/numOfInterval); // Avoid negative values
        return missingRate;
    }


}
