package networkConnection;

import Utility.util;
import analysis.dataAggregation;
import analysis.dataFiltering;
import analysis.healthAnalysis;
import main.MainFunction;
import saveData.saveDataToDatabase;

import java.sql.*;
import java.text.*;
import java.util.*;
import java.io.*;
import java.util.Date;

/**
 * Created by Qijian-Gan on 2/23/2018.
 */
public class automaticDetectorHealthAnalysis {
    // This is the main class to automatically analyze detector health and perform data filtering and imputation

    public static void updateArcadiaTCSData(){
        // Update the tables for Arcadia TCS Data

        // Get the current Year-Month-Day
        Calendar c=Calendar.getInstance();
        c.add( Calendar.DAY_OF_WEEK, -1 );
        int ToYear=c.get(Calendar.YEAR);
        int ToMonth=c.get(Calendar.MONTH)+1; // Month starts at 0, not 1
        int ToDay=c.get(Calendar.DAY_OF_MONTH);

        try {
            // Get the connection
            Connection con = DriverManager.getConnection(MainFunction.host, MainFunction.userName, MainFunction.password);
            // Get the unique Detector IDs
            String sql="SELECT distinct DetectorID FROM detector_data_raw_"+ToYear+" where DetectorID!=9901";
            Statement ps=con.createStatement();
            ResultSet resultSet=ps.executeQuery(sql);
            List<Integer> uniqueDetIDs=getUniqueDetectorIDsFromResultSet(resultSet);

            for(int i=0;i<uniqueDetIDs.size();i++){
                System.out.println(uniqueDetIDs.get(i));
                UpdateHealthTableForEachDetector(con, uniqueDetIDs.get(i), ToYear,ToMonth, ToDay);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static boolean UpdateHealthTableForEachDetector(Connection con, int DetectorID, int CurYear,int CurMonth, int CurDay) {
        // This function is used to update health table for each detector

        // First, get the latest update date in the detector health table
        try {
            Statement ps = con.createStatement();
            // Get the latest date in the detector health table
            String sql = "SELECT  max(Year*10000+Month*100+Day) as 'MaxDate'  FROM detector_health where DetectorID=" + DetectorID;
            ResultSet resultSet = ps.executeQuery(sql);
            int PastYear = 0;
            int PastMonth = 0;
            int PastDay = 0;
            while (resultSet.next()) {
                int maxDate=Math.max(PastYear, resultSet.getInt("MaxDate"));
                int yearMonth=maxDate/100;
                PastYear = yearMonth/100;
                PastMonth = yearMonth%100;
                PastDay = maxDate%100;
            }
            PastDay=PastDay+1; // Add one more day to start with

            // Get the min date in the raw data table
            sql= "SELECT min(Date) FROM detector_data_raw_"+CurYear+" where DetectorID="+DetectorID;
            resultSet = ps.executeQuery(sql);
            int PastDate=PastYear*10000+PastMonth*100+PastDay;
            while (resultSet.next()) {
                // Take the maximum of the two times to determine which one to start with
                PastDate = Math.max(PastDate, resultSet.getInt("min(Date)"));
            }

            // Get the thresholds
            healthAnalysis.HealthThreshold healthThreshold = new healthAnalysis.HealthThreshold(
                    MainFunction.cBlock.missingRateThreshold_TCSServer,
                    MainFunction.cBlock.maxZeroValueThreshold_TCSServer,
                    MainFunction.cBlock.highValueRateThreshold_TCSServer,
                    MainFunction.cBlock.highFlowValue_TCSServer,
                    MainFunction.cBlock.inconsisRateWithoutSpeedThreshold_TCSServer);
            String organization = "Arcadia";

            // Get the date ranges
            Date fromDate = new SimpleDateFormat("yyyyMMdd").parse(String.valueOf(PastDate));
            Date toDate = new SimpleDateFormat("yyyy-MM-dd").parse(CurYear + "-" + CurMonth + "-" + CurDay);
            int numDay = (int) Math.round((toDate.getTime() - fromDate.getTime()) / 1000.0 / 60.0 / 60.0 / 24.0) + 1;

            // Loop for each day
            Date tmpDate = fromDate;
            for (int i = 0; i < numDay; i++) {
                System.out.println(tmpDate);
                long start = System.currentTimeMillis();
                // Get the year,month,day for a given date
                int dateNum = util.dateToNumber(tmpDate, "DateNum");
                int year = util.dateToNumber(tmpDate, "Year");
                int month = util.dateToNumber(tmpDate, "Month");
                int day = util.dateToNumber(tmpDate, "Day");

                String sqlDate = null;
                sqlDate = "select Time,Volume,Occupancy,Speed from detector_data_raw_" + year + " where (Date="
                        + dateNum + " and DetectorID=" + DetectorID + ") order by Time;";
                Statement psData = con.createStatement();
                ResultSet resultData = psData.executeQuery(sqlDate);
                List<double[]> dataInputArray = util.convertResultsetToArray(resultData);
                if(dataInputArray.size()>0) {//Have data
                    double[][] dataInput = util.convertArrayToMatrix(dataInputArray);

                    // Detector health analysis
                    mainHealthAnalysis(con, organization, DetectorID,
                            MainFunction.cBlock.defaultInterval, year, month, day, healthThreshold, dataInput);
                    // Data filtering
                    mainDataFiltering(con, dataInput, DetectorID, year,
                            month, day, MainFunction.cBlock.defaultInterval);
                    long end = System.currentTimeMillis();
                    System.out.println("Total time taken = " + (end - start) + " ms");
                }
                tmpDate = util.addDays(tmpDate, 1);
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static List<Integer> getUniqueDetectorIDsFromResultSet(ResultSet resultSet){
        // This function is used to get unique detector IDs from result set

        List<Integer> uniqueDetIDs=new ArrayList<Integer>();
        try {
            while(resultSet.next()){
                uniqueDetIDs.add(resultSet.getInt("DetectorID"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return uniqueDetIDs;
    }


    public static void updateLACOArcadiaIENData(){
        // Update the tables for LACO IEN Data
        //Create two workers for Arcadia and LACO
        MyRunnableArcadia worker1 = new MyRunnableArcadia();
        Thread t1 = new Thread(worker1);
        t1.start();
        MyRunnableLACO worker2 = new MyRunnableLACO();
        Thread t2 = new Thread(worker2);
        t2.start();
    }


    public static class MyRunnableArcadia implements Runnable{
        // This is the class for runnable programs
        public void run()
        {
            aggregatedDataHealthAnalysisArcadia();
        }
    }
    public static boolean aggregatedDataHealthAnalysisArcadia(){

        // Get the current Year-Month-Day
        Calendar c=Calendar.getInstance();
        c.add( Calendar.DAY_OF_WEEK, -1 ); // Start Date= Yesterday
        int ToYear=c.get(Calendar.YEAR);
        int ToMonth=c.get(Calendar.MONTH)+1; // Month starts at 0, not 1
        int ToDay=c.get(Calendar.DAY_OF_MONTH);

        // First, get the latest update date in the detector health table
        try {
            // Get the connection
            Connection con = DriverManager.getConnection(MainFunction.hostIENArcadia, MainFunction.userName, MainFunction.password);
            Statement ps = con.createStatement();
            // Note: the following method assumes all detectors are updated together so the MAXDate will be the same
            // Get the latest date in the detector health table
            String sql = "SELECT  max(Year*10000+Month*100+Day) as 'MaxDate' FROM detector_health";
            ResultSet resultSet = ps.executeQuery(sql);
            int PastYear = 0;
            int PastMonth = 0;
            int PastDay = 0;
            while (resultSet.next()) {
                int maxDate=Math.max(PastYear, resultSet.getInt("MaxDate"));
                int yearMonth=maxDate/100;
                PastYear = yearMonth/100;
                PastMonth = yearMonth%100;
                PastDay = maxDate%100;
            }
            PastDay=PastDay+1; // Add one more day to start with
            int PastDate=PastYear*10000+PastMonth*100+PastDay;

            // Get the date ranges
            DateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
            Date fromDate = new SimpleDateFormat("yyyyMMdd").parse(String.valueOf(PastDate));
            String fromDateString=dateFormat.format(fromDate);
            String toDateString = ToYear + "-" + ToMonth + "-" + ToDay;
            Date toDate = new SimpleDateFormat("yyyy-MM-dd").parse(toDateString);

            // Get the date ranges
            int numDay = (int) Math.round((toDate.getTime() - fromDate.getTime()) / 1000.0 / 60.0 / 60.0 / 24.0)+1;
            if(numDay>0) {
                // Aggregating the data
                System.out.println("Aggregating detector data for Arcadia!");
                List<dataAggregation.aggregatedDetectorData> aggregatedData =
                        dataAggregation.aggregationToLongerIntervalHistorical(con,
                                MainFunction.cBlock.interval, fromDateString, toDateString);
                System.out.println("Saving aggregated detector data for Arcadia!");
                saveDataToDatabase.insertAggregatedDetectorDataToDataBaseBatch(con, aggregatedData);
                System.out.println("Done for Arcadia!");

                // Performing detector health analysis
                // Get the thresholds
                healthAnalysis.HealthThreshold healthThreshold = new healthAnalysis.HealthThreshold(MainFunction.cBlock.missingRateThreshold_IEN,
                        MainFunction.cBlock.maxZeroValueThreshold_IEN,
                        MainFunction.cBlock.highValueRateThreshold_IEN,
                        MainFunction.cBlock.highFlowValue_IEN,
                        MainFunction.cBlock.inconsisRateWithoutSpeedThreshold_IEN);
                String organization = "Arcadia";

                // Loop for each day
                Date tmpDate = fromDate;
                for (int i = 0; i < numDay; i++) {
                    System.out.println(tmpDate);
                    long start = System.currentTimeMillis();
                    // Get the year,month,day for a given date
                    int dateNum = util.dateToNumber(tmpDate, "DateNum");
                    int year = util.dateToNumber(tmpDate, "Year");
                    int month = util.dateToNumber(tmpDate, "Month");
                    int day = util.dateToNumber(tmpDate, "Day");

                    // First get the number of detectors
                    sql = "select DISTINCT(DetectorID) from detector_aggregated_data_" + year + " where Date=" + dateNum + ";";
                    ResultSet result = ps.executeQuery(sql);
                    while (result.next()) {
                        int detectorID = result.getInt("DetectorID");
                        if (detectorID != 9901) {
                            //System.out.println(tmpDate+" and Detector="+detectorID);
                            // Get the raw data
                            String sqlDate = null;
                            sqlDate = "select Time,Volume,Occupancy,Speed from detector_aggregated_data_" + year + " where (Date="
                                    + dateNum + " and DetectorID=" + detectorID + ") order by Time;";
                            Statement psData = con.createStatement();
                            ResultSet resultData = psData.executeQuery(sqlDate);
                            List<double[]> dataInputArray = util.convertResultsetToArray(resultData);
                            if (dataInputArray.size() > 0) {
                                double[][] dataInput = util.convertArrayToMatrix(dataInputArray);
                                // Detector health analysis
                                mainHealthAnalysis(con, organization, detectorID, MainFunction.cBlock.defaultInterval, year, month, day, healthThreshold, dataInput);
                                // Data filtering
                                dataFiltering.mainDataFiltering(con, dataInput, detectorID, year, month, day, MainFunction.cBlock.defaultInterval);
                            }
                        }
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("Total time taken = " + (end - start) + " ms");
                    tmpDate = util.addDays(tmpDate, 1);
                }
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }

    }

    public static class MyRunnableLACO implements Runnable{
        // This is the class for runnable programs
        public void run()
        {
            aggregatedDataHealthAnalysisLACO();
        }
    }
    public static boolean aggregatedDataHealthAnalysisLACO(){

        // Get the current Year-Month-Day
        Calendar c=Calendar.getInstance();
        c.add( Calendar.DAY_OF_WEEK, -1 ); // Start Date= Yesterday
        int ToYear=c.get(Calendar.YEAR);
        int ToMonth=c.get(Calendar.MONTH)+1; // Month starts at 0, not 1
        int ToDay=c.get(Calendar.DAY_OF_MONTH);

        // First, get the latest update date in the detector health table
        try {
            // Get the connection
            Connection con = DriverManager.getConnection(MainFunction.hostIENLACO, MainFunction.userName, MainFunction.password);
            Statement ps = con.createStatement();
            // Note: the following method assumes all detectors are updated together so the MAXDate will be the same
            // Get the latest date in the detector health table
            String sql = "SELECT  max(Year*10000+Month*100+Day) as 'MaxDate' FROM detector_health";
            ResultSet resultSet = ps.executeQuery(sql);
            int PastYear = 0;
            int PastMonth = 0;
            int PastDay = 0;
            while (resultSet.next()) {
                int maxDate=Math.max(PastYear, resultSet.getInt("MaxDate"));
                int yearMonth=maxDate/100;
                PastYear = yearMonth/100;
                PastMonth = yearMonth%100;
                PastDay = maxDate%100;
            }
            PastDay=PastDay+1; // Add one more day to start with
            int PastDate=PastYear*10000+PastMonth*100+PastDay;

            // Get the date ranges
            DateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
            Date fromDate = new SimpleDateFormat("yyyyMMdd").parse(String.valueOf(PastDate));
            String fromDateString=dateFormat.format(fromDate);
            String toDateString = ToYear + "-" + ToMonth + "-" + ToDay;
            Date toDate = new SimpleDateFormat("yyyy-MM-dd").parse(toDateString);

            // Get the date ranges
            int numDay = (int) Math.round((toDate.getTime() - fromDate.getTime()) / 1000.0 / 60.0 / 60.0 / 24.0)+1;
            if(numDay>0) {
                // Aggregating the data
                System.out.println("Aggregating detector data for LACO!");
                List<dataAggregation.aggregatedDetectorData> aggregatedData =
                        dataAggregation.aggregationToLongerIntervalHistorical(con,
                                MainFunction.cBlock.interval, fromDateString, toDateString);
                System.out.println("Saving aggregated detector data for LACO!");
                saveDataToDatabase.insertAggregatedDetectorDataToDataBaseBatch(con, aggregatedData);
                System.out.println("Done for LACO!");

                // Performing detector health analysis
                // Get the thresholds
                healthAnalysis.HealthThreshold healthThreshold = new healthAnalysis.HealthThreshold(MainFunction.cBlock.missingRateThreshold_IEN,
                        MainFunction.cBlock.maxZeroValueThreshold_IEN,
                        MainFunction.cBlock.highValueRateThreshold_IEN,
                        MainFunction.cBlock.highFlowValue_IEN,
                        MainFunction.cBlock.inconsisRateWithoutSpeedThreshold_IEN);
                String organization = "LACO";

                // Loop for each day
                Date tmpDate = fromDate;
                for (int i = 0; i < numDay; i++) {
                    System.out.println(tmpDate);
                    long start = System.currentTimeMillis();
                    // Get the year,month,day for a given date
                    int dateNum = util.dateToNumber(tmpDate, "DateNum");
                    int year = util.dateToNumber(tmpDate, "Year");
                    int month = util.dateToNumber(tmpDate, "Month");
                    int day = util.dateToNumber(tmpDate, "Day");

                    // First get the number of detectors
                    sql = "select DISTINCT(DetectorID) from detector_aggregated_data_" + year + " where Date=" + dateNum + ";";
                    ResultSet result = ps.executeQuery(sql);
                    while (result.next()) {
                        int detectorID = result.getInt("DetectorID");
                        if (detectorID != 9901) {
                            //System.out.println(tmpDate+" and Detector="+detectorID);
                            // Get the raw data
                            String sqlDate = null;
                            sqlDate = "select Time,Volume,Occupancy,Speed from detector_aggregated_data_" + year + " where (Date="
                                    + dateNum + " and DetectorID=" + detectorID + ") order by Time;";
                            Statement psData = con.createStatement();
                            ResultSet resultData = psData.executeQuery(sqlDate);
                            List<double[]> dataInputArray = util.convertResultsetToArray(resultData);
                            if (dataInputArray.size() > 0) {
                                double[][] dataInput = util.convertArrayToMatrix(dataInputArray);
                                // Detector health analysis
                                mainHealthAnalysis(con, organization, detectorID, MainFunction.cBlock.defaultInterval, year, month, day, healthThreshold, dataInput);
                                // Data filtering
                                dataFiltering.mainDataFiltering(con, dataInput, detectorID, year, month, day, MainFunction.cBlock.defaultInterval);
                            }
                        }
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("Total time taken = " + (end - start) + " ms");
                    tmpDate = util.addDays(tmpDate, 1);
                }
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }

    }


    public static void mainHealthAnalysis(Connection con, String organization,int detectorID, int interval,int year, int month,int day, healthAnalysis.HealthThreshold healthThreshold,
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
            healthAnalysis.DetectorHealthMetrics detectorHealthMetrics=
                    healthAnalysis.checkDetectorHealthByDay(interval, dataInput,healthThreshold,numOfLane);
            // Save the health analysis to the database
            insertHealthMeasurementToDataBase(con, detectorHealthMetrics,detectorID,year,month,day);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static boolean insertHealthMeasurementToDataBase(Connection con, healthAnalysis.DetectorHealthMetrics detectorHealthMetrics,
                                                            int detectorID, int year,int month, int day){
        // This function is used to insert health data into database

        try {
            Statement ps=con.createStatement();
            String sql="insert into detector_health values ("+detectorID+","+year+
                    ","+month+","+day+","+detectorHealthMetrics.MissingRate+","+detectorHealthMetrics.MaxZeroValue+","
                    +detectorHealthMetrics.HighValueRate+","+detectorHealthMetrics.ConstantOrNot+
                    ","+detectorHealthMetrics.InconsisRateWithoutSpeed+","+detectorHealthMetrics.Health+");";

            ps.execute(sql);
            return true;
        } catch (SQLException e) {
            System.out.println("Fail to insert:"+ e.getMessage());
            return false;
        }
    }



    public static void mainDataFiltering(Connection con, double [][] dataInput, int detectorID, int year, int month, int day, int interval){

        // Fill in missing values
        dataFiltering.ImputationSetting imputationSetting= new dataFiltering.ImputationSetting(MainFunction.cBlock.spanImputation,
                MainFunction.cBlock.useMedianOrNotImputation);

        double [][] dataFillInMissingValue=dataFiltering.fillInMissingValues(dataInput,interval,imputationSetting);

        // Smooth the data
        dataFiltering.SmoothSetting smoothSetting= new dataFiltering.SmoothSetting(MainFunction.cBlock.methodSmoothing,MainFunction.cBlock.spanSmoothing);
        double [][] dataSmooth=dataFiltering.smoothingData(dataFillInMissingValue,smoothSetting);

        // Insert the processed data to database
        insertProcessedTCSDataToDataBase(con, dataSmooth, detectorID, year, month,day);
    }

    public static boolean insertProcessedTCSDataToDataBase(Connection con, double [][] dataInput, int detectorID, int year, int month, int day){
        // This function is used to insert processed TCS data to the database
        try {
            // Create a Statement from the connection
            Statement ps=con.createStatement();
            List<String> string= new ArrayList<String>();
            for(int i=0;i<dataInput.length;i++) {
                String sql = "insert into detector_data_processed_"+year+" values ("+detectorID+","+year+
                        ","+month+","+day+","+(int) dataInput[i][0]+","+dataInput[i][1]+","+dataInput[i][2]+","+dataInput[i][3]+");";
                string.add(sql);
            }
            if(string.size()>0){
                saveDataToDatabase.insertSQLBatch(ps, string,100);
            }
            return true;
        } catch (SQLException e) {
            //If fail
            System.out.println("Fail to insert the data!");
            return false;
        }
    }

}
