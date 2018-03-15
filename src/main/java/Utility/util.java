package Utility;


import loadData.readIENData;
import main.MainFunction;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.io.File;
import java.util.HashSet;

/**
 * Created by Qijian_Gan on 9/12/2017.
 */
public class util
{
    // ***********************************************************//
    // This is the main function to clean the tables for testing
    public static void mainTruncateTables(String type,String mode, int dataYear){
        if(type.equals("IENArcadia")) {
            System.out.println("IEN Arcadia Database");
            util.truncateTables(MainFunction.hostIENArcadia, MainFunction.cBlock.userName, MainFunction.cBlock.password, mode, dataYear);
        }else if(type.equals("IENLACO")){
            System.out.println("IEN LACO Database");
            util.truncateTables(MainFunction.hostIENLACO, MainFunction.cBlock.userName, MainFunction.cBlock.password, mode, dataYear);
        }else if(type.equals("Both")){
            System.out.println("IEN Arcadia Database");
            util.truncateTables(MainFunction.hostIENArcadia, MainFunction.cBlock.userName, MainFunction.cBlock.password, mode, dataYear);
            System.out.println("IEN LACO Database");
            util.truncateTables(MainFunction.hostIENLACO, MainFunction.cBlock.userName, MainFunction.cBlock.password, mode, dataYear);
        }
    }

    public static readIENData.uniqueDataWithHashSet removeDuplicatedData_rev(readIENData.uniqueDataWithHashSet inputData){
        // This function is used to remove duplicated data

        readIENData.uniqueDataWithHashSet outputData=readIENData.initilizationUniqueDataWithHashSet();

        readIENData.IENData ienData=inputData.ienData;
        // Detector Data
        List<String> string = new ArrayList<String>();
        if(ienData.listDevData.size()>0){// If not empty
            for(int i=0;i<ienData.listDevData.size();i++){ //For each item, create the string
                string.add(ienData.listDevData.get(i).orgID+ienData.listDevData.get(i).devID
                        +ienData.listDevData.get(i).date+ienData.listDevData.get(i).time);
            }
            HashSet<String> set = inputData.setDevData; // Create the hash set
            String arrayElement;
            for (int i=0;i<string.size();i++) // Loop for each row of strings
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))// Able to add to the hash set
                {// Copy the unique item
                    outputData.ienData.listDevData.add(ienData.listDevData.get(i));
                    outputData.setDevData.add(arrayElement);
                }
            }
        }

        // Detector Inventory
        string = new ArrayList<String>();
        if(ienData.listDevInv.size()>0){
            for(int i=0;i<ienData.listDevInv.size();i++){
                string.add(ienData.listDevInv.get(i).orgID+ienData.listDevInv.get(i).devID
                        +ienData.listDevInv.get(i).date+ienData.listDevInv.get(i).time);
            }
            HashSet<String> set = inputData.setDevInv;
            String arrayElement;
            for (int i=0;i<string.size();i++)
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))
                {
                    outputData.ienData.listDevInv.add(ienData.listDevInv.get(i));
                    outputData.setDevInv.add(arrayElement);
                }
            }
        }

        // Intersection Signal Inventory
        string = new ArrayList<String>();
        if(ienData.listIntSigInv.size()>0){
            for(int i=0;i<ienData.listIntSigInv.size();i++){
                string.add(ienData.listIntSigInv.get(i).orgID+ienData.listIntSigInv.get(i).intID
                        +ienData.listIntSigInv.get(i).date+ienData.listIntSigInv.get(i).time);
            }
            HashSet<String> set = inputData.setIntSigInv;
            String arrayElement;
            for (int i=0;i<string.size();i++)
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))
                {
                    outputData.ienData.listIntSigInv.add(ienData.listIntSigInv.get(i));
                    outputData.setIntSigInv.add(arrayElement);
                }
            }
        }

        // Intersection Signal data
        string = new ArrayList<String>();
        if(ienData.listIntSigData.size()>0){
            for(int i=0;i<ienData.listIntSigData.size();i++){
                string.add(ienData.listIntSigData.get(i).orgID+ienData.listIntSigData.get(i).intID
                        +ienData.listIntSigData.get(i).date+ienData.listIntSigData.get(i).time);
            }
            HashSet<String> set = inputData.setIntSigData;
            String arrayElement;
            for (int i=0;i<string.size();i++)
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))
                {
                    outputData.ienData.listIntSigData.add(ienData.listIntSigData.get(i));
                    outputData.setIntSigData.add(arrayElement);
                }
            }
        }

        // Signal planned phase data
        string = new ArrayList<String>();
        if(ienData.listPlanPhase.size()>0){
            for(int i=0;i<ienData.listPlanPhase.size();i++){
                string.add(ienData.listPlanPhase.get(i).orgID+ienData.listPlanPhase.get(i).intID
                        +ienData.listPlanPhase.get(i).date+ienData.listPlanPhase.get(i).time);
            }
            HashSet<String> set = inputData.setPlanPhase;
            String arrayElement;
            for (int i=0;i<string.size();i++)
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))
                {
                    outputData.ienData.listPlanPhase.add(ienData.listPlanPhase.get(i));
                    outputData.setPlanPhase.add(arrayElement);
                }
            }
        }

        // Signal last-cycle phase data
        string = new ArrayList<String>();
        if(ienData.listLastCyclePhase.size()>0){
            for(int i=0;i<ienData.listLastCyclePhase.size();i++){
                string.add(ienData.listLastCyclePhase.get(i).orgID+ienData.listLastCyclePhase.get(i).intID
                        +ienData.listLastCyclePhase.get(i).date+ienData.listLastCyclePhase.get(i).time);
            }
            HashSet<String> set = inputData.setLastCyclePhase;
            String arrayElement;
            for (int i=0;i<string.size();i++)
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))
                {
                    outputData.ienData.listLastCyclePhase.add(ienData.listLastCyclePhase.get(i));
                    outputData.setLastCyclePhase.add(arrayElement);
                }
            }
        }

        return outputData;
    }

    public static readIENData.IENData removeDuplicatedData(readIENData.IENData ienData){
        // This function is used to remove duplicated data
        readIENData.IENData ienData1=readIENData.inilizationIENData();

        // Detector Data
        List<String> string = new ArrayList<String>();
        if(ienData.listDevData.size()>0){// If not empty
            for(int i=0;i<ienData.listDevData.size();i++){ //For each item, create the string
                string.add(ienData.listDevData.get(i).orgID+ienData.listDevData.get(i).devID
                        +ienData.listDevData.get(i).date+ienData.listDevData.get(i).time);
            }
            HashSet<String> set = new HashSet<String>(); // Create the hash set
            String arrayElement;
            for (int i=0;i<string.size();i++) // Loop for each row of strings
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))// Able to add to the hash set
                {// Copy the unique item
                    ienData1.listDevData.add(ienData.listDevData.get(i));
                }
            }
        }

        // Detector Inventory
        string = new ArrayList<String>();
        if(ienData.listDevInv.size()>0){
            for(int i=0;i<ienData.listDevInv.size();i++){
                string.add(ienData.listDevInv.get(i).orgID+ienData.listDevInv.get(i).devID
                        +ienData.listDevInv.get(i).date+ienData.listDevInv.get(i).time);
            }
            HashSet<String> set = new HashSet<String>();
            String arrayElement;
            for (int i=0;i<string.size();i++)
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))
                {
                    ienData1.listDevInv.add(ienData.listDevInv.get(i));
                }
            }
        }

        // Intersection Signal Inventory
        string = new ArrayList<String>();
        if(ienData.listIntSigInv.size()>0){
            for(int i=0;i<ienData.listIntSigInv.size();i++){
                string.add(ienData.listIntSigInv.get(i).orgID+ienData.listIntSigInv.get(i).intID
                        +ienData.listIntSigInv.get(i).date+ienData.listIntSigInv.get(i).time);
            }
            HashSet<String> set = new HashSet<String>();
            String arrayElement;
            for (int i=0;i<string.size();i++)
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))
                {
                    ienData1.listIntSigInv.add(ienData.listIntSigInv.get(i));
                }
            }
        }

        // Intersection Signal data
        string = new ArrayList<String>();
        if(ienData.listIntSigData.size()>0){
            for(int i=0;i<ienData.listIntSigData.size();i++){
                string.add(ienData.listIntSigData.get(i).orgID+ienData.listIntSigData.get(i).intID
                        +ienData.listIntSigData.get(i).date+ienData.listIntSigData.get(i).time);
            }
            HashSet<String> set = new HashSet<String>();
            String arrayElement;
            for (int i=0;i<string.size();i++)
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))
                {
                    ienData1.listIntSigData.add(ienData.listIntSigData.get(i));
                }
            }
        }

        // Signal planned phase data
        string = new ArrayList<String>();
        if(ienData.listPlanPhase.size()>0){
            for(int i=0;i<ienData.listPlanPhase.size();i++){
                string.add(ienData.listPlanPhase.get(i).orgID+ienData.listPlanPhase.get(i).intID
                        +ienData.listPlanPhase.get(i).date+ienData.listPlanPhase.get(i).time);
            }
            HashSet<String> set = new HashSet<String>();
            String arrayElement;
            for (int i=0;i<string.size();i++)
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))
                {
                    ienData1.listPlanPhase.add(ienData.listPlanPhase.get(i));
                }
            }
        }

        // Signal last-cycle phase data
        string = new ArrayList<String>();
        if(ienData.listLastCyclePhase.size()>0){
            for(int i=0;i<ienData.listLastCyclePhase.size();i++){
                string.add(ienData.listLastCyclePhase.get(i).orgID+ienData.listLastCyclePhase.get(i).intID
                        +ienData.listLastCyclePhase.get(i).date+ienData.listLastCyclePhase.get(i).time);
            }
            HashSet<String> set = new HashSet<String>();
            String arrayElement;
            for (int i=0;i<string.size();i++)
            {
                arrayElement=string.get(i);
                if(set.add(arrayElement))
                {
                    ienData1.listLastCyclePhase.add(ienData.listLastCyclePhase.get(i));
                }
            }
        }

        return ienData1;
    }

    public static boolean truncateTables(String host, String userName, String password, String mode, int dataYear){
        // This function is used to truncate the tables in the databases

        try{
            Connection con= DriverManager.getConnection(host, userName, password);
            Statement ps = con.createStatement();

            if(mode.equals("All"))
            {
                String sql="truncate plan_phase_"+dataYear;
                ps.execute(sql);
                System.out.println(sql);

                sql="truncate last_cycle_phase_"+dataYear;
                ps.execute(sql);
                System.out.println(sql);

                sql="truncate intersection_signal_inventory";
                ps.execute(sql);
                System.out.println(sql);

                sql="truncate intersection_signal_data_"+dataYear;
                ps.execute(sql);
                System.out.println(sql);

                sql="truncate detector_raw_data_"+dataYear;
                ps.execute(sql);
                System.out.println(sql);

                sql="truncate detector_inventory_ien";
                ps.execute(sql);
                System.out.println(sql);
            }else if(mode.equals("SignalPhase")){
                String sql="truncate plan_phase_"+dataYear;
                ps.execute(sql);
                System.out.println(sql);

                sql="truncate last_cycle_phase_"+dataYear;
                ps.execute(sql);
                System.out.println(sql);
            }else if(mode.equals("DetectorData")){
                String sql="truncate detector_raw_data_"+dataYear;
                ps.execute(sql);
                System.out.println(sql);

                sql="truncate detector_inventory_ien";
                ps.execute(sql);
                System.out.println(sql);
            }else if(mode.equals("IntersectionSignal")){
                String sql="truncate intersection_signal_inventory";
                ps.execute(sql);
                System.out.println(sql);

                sql="truncate intersection_signal_data_"+dataYear;
                ps.execute(sql);
                System.out.println(sql);
            }else if(mode.equals("AggregatedDetectorData")){
                String sql="truncate detector_aggregated_data_"+dataYear;
                ps.execute(sql);
                System.out.println(sql);
            }else{
                System.out.println("Unknown mode to truncate tables!");
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static double[][] convertArrayToMatrix(List<double[]> dataInputArray){
        double [][] dataOutput=new double[dataInputArray.size()][dataInputArray.get(0).length];

        for (int i=0;i<dataInputArray.size();i++){
            dataOutput[i]=dataInputArray.get(i);
        }
        return dataOutput;
    }

    public static List<double[]> convertResultsetToArray(ResultSet resultSet){

        List<double[]> dataOutput=new ArrayList<double[]>();

        try{
            ResultSetMetaData rsmd=resultSet.getMetaData();
            int Column=rsmd.getColumnCount();
            while(resultSet.next()){
                double[] tmp = new double[4];
                if(Column==4) {
                    tmp[0] = resultSet.getDouble("Time");
                    tmp[1] = resultSet.getDouble("Volume");
                    tmp[2] = resultSet.getDouble("Occupancy");
                    tmp[3] = resultSet.getDouble("Speed");
                }else if(Column==5){ // Contains the field "Period"
                    int Period=resultSet.getInt("Period");
                    tmp[0] = resultSet.getDouble("Time");
                    tmp[1] = resultSet.getDouble("Volume")* ((double) 60.0/Period); // Aggregate to hourly based
                    tmp[2] = resultSet.getDouble("Occupancy");
                    tmp[3] = resultSet.getDouble("Speed");
                }else{
                    System.out.println("Not the right number of fields in the result set!");
                    System.exit(-1);
                }
                dataOutput.add(tmp);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dataOutput;
    }

    public static int dateToNumber(Date tmpDate, String type){

        SimpleDateFormat Year= new SimpleDateFormat("yyyy");
        SimpleDateFormat Month= new SimpleDateFormat("MM");
        SimpleDateFormat Day= new SimpleDateFormat("dd");

        int year= new Integer(Year.format(tmpDate));
        int month=new Integer(Month.format(tmpDate));
        int day=new Integer(Day.format(tmpDate));

        int dateNum=0;
        if(type.equals("DateNum"))
            dateNum=year*10000+month*100+day;
        else if(type.equals("Year"))
            dateNum=year;
        else if(type.equals("Month"))
            dateNum=month;
        else if(type.equals("Day"))
            dateNum=day;
        else
            System.out.println("Unknown date output type!");

        return dateNum;
    }

    public static Date addDays(Date date, int days)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days); //minus number would decrement the days
        return cal.getTime();
    }

    public static double calculateMean(double[] inputArray) {
        // This function is to calculate the mean value

        double sum = 0;
        for (int i = 0; i < inputArray.length; i++) {
            sum += inputArray[i];
        }
        return sum / inputArray.length;
    }

    public static double calculateMedian(double [] inputArray){
        // This function is to calcualte the median value

        double median;

        // Sort the array
        Arrays.sort(inputArray);

        if (inputArray.length % 2 == 0) // If it is even number
            median = (inputArray[inputArray.length/2] + inputArray[inputArray.length/2 - 1])/2;
        else // If it is odd number
            median = inputArray[inputArray.length/2];

        return median;
    }

    public static double [] fromMatrixToArrayByColumn(double [][] inputMatrix, int colNumber, int fromRow, int toRow){
        // This function is to assign the value from a matrix to an array with a given column number

        double [] outputArray= new double[toRow-fromRow+1];

        for(int i=0; i<=toRow-fromRow;i++) {
            outputArray[i]=inputMatrix[i+fromRow][colNumber];
        }

        return outputArray;
    }

    public static boolean moveFileFromAToB(String fromFolder, String toFolder, String fileName){
        // This function is used to move file from fromFolder to toFolder


        File afileDir= new File(fromFolder);
        File afile = new File(afileDir,fileName);

        File bfileDir= new File(toFolder);
        File bfile = new File(bfileDir,fileName);

        if(afile.renameTo(bfile)){
            //System.out.println("File removed!");
            return true;
        }else{
            System.out.println("Fail to remove file:"+afile.getName());
            return false;
        }
    }

    public static double [] copyValueWithoutPointerDouble(double [] input){
        double [] output=new double [input.length];
        for (int i=0;i<input.length;i++){
            output[i]=input[i];
        }
        return output;
    }

    public static int getTheSumInteger(int [] Input){
        int Output=0;
        for(int i=0;i<Input.length;i++){
            Output=Output+Input[i];
        }
        return Output;
    }
}