package saveData;

import Utility.util;
import analysis.healthAnalysis;
import analysis.dataAggregation;
import config.detectorConfig;
import loadData.loadTCSDetectorData;
import loadData.readIENData;
import loadData.loadPasadenaData;
import loadData.*;
import main.MainFunction;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Created by Qijian_Gan on 9/12/2017.
 */
public class saveDataToDatabase{
    public static void mainLoadTCSSignalPhaseDataToDataBase(){
        // This function is used to load TCS signal data into database

        //Create two workers for Phase Times
        MyRunnablePhaseTimes worker1= new MyRunnablePhaseTimes();
        Thread t1 = new Thread(worker1);
        t1.start();

        //Create two workers for timing plans
        MyRunnableTimingPlans worker2= new MyRunnableTimingPlans();
        Thread t2 = new Thread(worker2);
        t2.start();
    }
    public static class MyRunnablePhaseTimes implements Runnable{
        // This is the class for runnable programs
        public void run(){
            loadPhasetimesThread();
        }
    }
    public static void loadPhasetimesThread(){
        // This function is used to load phase times
        try {
            File fileDir = new File(MainFunction.cBlock.phaseTimeDataDir);
            File[] listOfFiles = fileDir.listFiles();
            MainFunction.conTCSServer = DriverManager.getConnection(MainFunction.host, MainFunction.userName, MainFunction.password);
            System.out.println("Succefully connect to the database!");

            // Load files to the database
            int maxNumberLoadFile=100;
            int curNumberLoadFile=0;
            List<loadTCSSignalData.PhaseTimes> phaseTimesList=new ArrayList<loadTCSSignalData.PhaseTimes>();
            for (int i = 0; i < listOfFiles.length; i++) {
                // Get the list of files
                if (listOfFiles[i].isFile()) {
                    // If it is a file
                    List<loadTCSSignalData.PhaseTimes> tmpPhaseTimesList=loadTCSSignalData.loadPhaseTimes(listOfFiles[i]);
                    phaseTimesList.addAll(tmpPhaseTimesList);
                    curNumberLoadFile=curNumberLoadFile+1;
                } else if (listOfFiles[i].isDirectory()) {
                    // If it is a directory
                    System.out.println("Directory " + listOfFiles[i].getName());
                }
                if(curNumberLoadFile==maxNumberLoadFile || i == listOfFiles.length-1){
                    insertTCSPhaseTimeDataToDataBase(MainFunction.conTCSServer,phaseTimesList);
                    curNumberLoadFile=0;
                    phaseTimesList=new ArrayList<loadTCSSignalData.PhaseTimes>();
                }
            }

            if(!MainFunction.cBlock.phaseTimeDataNewDir.equals("")){
                // Remove files
                for (int i = 0; i < listOfFiles.length; i++) {
                    // Get the list of files
                    util.moveFileFromAToB(MainFunction.cBlock.phaseTimeDataDir,
                            MainFunction.cBlock.phaseTimeDataNewDir, listOfFiles[i].getName());
                }
            }

        }catch (SQLException e)
        {
            e.printStackTrace();
        }
    }
    public static class MyRunnableTimingPlans implements Runnable{
        // This is the class for runnable programs
        public void run(){
            loadTimingPlansThread();
        }
    }
    public static void loadTimingPlansThread(){
        // This function is used to load phase times
        try {
            File fileDir = new File(MainFunction.cBlock.timingPlansDataDir);
            File[] listOfFiles = fileDir.listFiles();
            MainFunction.conTCSServer = DriverManager.getConnection(MainFunction.host, MainFunction.userName, MainFunction.password);
            System.out.println("Succefully connect to the database!");

            // Load files to the database
            int maxNumberLoadFile=100;
            int curNumberLoadFile=0;
            List<loadTCSSignalData.TimingPlans> timingPlansList=new ArrayList<loadTCSSignalData.TimingPlans>();
            for (int i = 0; i < listOfFiles.length; i++) {
                // Get the list of files
                if (listOfFiles[i].isFile()) {
                    // If it is a file
                    List<loadTCSSignalData.TimingPlans> tmpTimingPlansList=loadTCSSignalData.loadTimingPlans(listOfFiles[i]);
                    timingPlansList.addAll(tmpTimingPlansList);
                    curNumberLoadFile=curNumberLoadFile+1;
                } else if (listOfFiles[i].isDirectory()) {
                    // If it is a directory
                    System.out.println("Directory " + listOfFiles[i].getName());
                }
                if(curNumberLoadFile==maxNumberLoadFile || i == listOfFiles.length-1){
                    insertTCSTimingPlansDataToDataBase(MainFunction.conTCSServer,timingPlansList);
                    curNumberLoadFile=0;
                    timingPlansList=new ArrayList<loadTCSSignalData.TimingPlans>();
                }
            }

            if(!MainFunction.cBlock.timingPlansDataNewDir.equals("")){
                // Remove files
                for (int i = 0; i < listOfFiles.length; i++) {
                    // Get the list of files
                    util.moveFileFromAToB(MainFunction.cBlock.timingPlansDataDir,
                            MainFunction.cBlock.timingPlansDataNewDir, listOfFiles[i].getName());
                }
            }

        }catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    public static void mainLoadTCSRawDataToDataBase(){
        try {
            File fileDir = new File(MainFunction.cBlock.rawTCSDataDir);
            File[] listOfFiles = fileDir.listFiles();
            MainFunction.conTCSServer = DriverManager.getConnection(MainFunction.host, MainFunction.userName, MainFunction.password);
            System.out.println("Succefully connect to the database!");

            // Load files to the database
            for (int i = 0; i < listOfFiles.length; i++) {
                // Get the list of files
                if (listOfFiles[i].isFile()) {
                    // If it is a file
                    System.out.println("File: " + listOfFiles[i].getName());
                    saveDataToDatabase.insertRawTCSDataToDataBase(MainFunction.conTCSServer,listOfFiles[i]);
                } else if (listOfFiles[i].isDirectory()) {
                    // If it is a directory
                    System.out.println("Directory " + listOfFiles[i].getName());
                }
            }

            if(!MainFunction.cBlock.rawTCSDataDir.equals("")){
                // Remove files
                for (int i = 0; i < listOfFiles.length; i++) {
                    // Get the list of files
                    util.moveFileFromAToB(MainFunction.cBlock.rawTCSDataDir, MainFunction.cBlock.rawTCSDataNewDir, listOfFiles[i].getName());
                }
            }

        }catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    public static void mainInsertDetConfigToDataBase(String type) {
        // This function is used to insert detector configuration to database
        if(type.equals("TCSServer")) {
            // TCS server
            saveDataToDatabase.insertDetectorConfigToDataBase(MainFunction.host, MainFunction.cBlock.userName,
                    MainFunction.cBlock.password, type, MainFunction.cBlock.configDirArcadia, MainFunction.cBlock.configNameArcadia);
        }else if(type.equals("IENArcadia")){
            // IEN Arcadia
            saveDataToDatabase.insertDetectorConfigToDataBase(MainFunction.hostIENArcadia,MainFunction.cBlock.userName,
                    MainFunction.cBlock.password, type,MainFunction.cBlock.configDirArcadia,MainFunction.cBlock.configNameArcadia);
        }else if(type.equals("IENLACO")) {
            // IEN LACO
            saveDataToDatabase.insertDetectorConfigToDataBase(MainFunction.hostIENLACO, MainFunction.cBlock.userName,
                    MainFunction.cBlock.password, type, MainFunction.cBlock.configDirLACO, MainFunction.cBlock.configNameLACO);
        }else{
            System.out.println("Wrong configuration of database name!");
        }
    }

    public static void mainLoadHealthFileToDataBase(){
        try {
            File fileDir = new File(MainFunction.cBlock.configDirHealth);
            File[] listOfFiles = fileDir.listFiles();

            Connection con = DriverManager.getConnection(MainFunction.host,
                    MainFunction.cBlock.userName, MainFunction.cBlock.password);
            System.out.println("Succefully connect to the database!");

            for (int i = 0; i < listOfFiles.length; i++) {
                // Get the list of files
                if (listOfFiles[i].isFile()) {
                    // If it is a file
                    System.out.println("File: " + listOfFiles[i].getName());
                    saveDataToDatabase.insertHealthFileToDataBase(con,listOfFiles[i]);
                } else if (listOfFiles[i].isDirectory()) {
                    // If it is a directory
                    System.out.println("Directory " + listOfFiles[i].getName());
                }
            }
        }catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    public static void insertPasadenaDataToDataBase(List<loadPasadenaData.DetectorData> detectorDataList){
        // This function is used to insert pasadena data to database

        try {
            Connection conPasadena = DriverManager.getConnection(MainFunction.hostPasadena,
                    MainFunction.cBlock.userName, MainFunction.cBlock.password);
            System.out.println("Succefully connect to the database!");

            String Header="insert into pasadena_data.detector_raw_data_";
            int year;
            if(detectorDataList.size()>0){// Not empty
                String sql;
                String arrayElement;
                List<String> stringPasadena= new ArrayList<String>();
                Statement psPasadena = conPasadena.createStatement();

                HashSet<String> stringHashSet= new HashSet<String>();; // Create the hash set
                for (int i=0;i<detectorDataList.size();i++) // Loop for each row
                {
                    loadPasadenaData.DetectorData detectorData=detectorDataList.get(i);
                    arrayElement=detectorData.DetectorID + "-" + detectorData.IntersectionID + "-" +
                            detectorData.Date + "-" + detectorData.Time;
                    year=detectorData.Date/10000;
                    sql =  Header+ year + " values (\"" +
                            detectorData.DetectorID + "\",\"" + detectorData.IntersectionID + "\",\"" +
                            detectorData.Date + "\",\"" + detectorData.Time + "\",\"" +
                            detectorData.Period + "\",\"" + detectorData.Volume + "\",\"" +
                            detectorData.Occupancy + "\",\"" + detectorData.Speed + "\",\"" +
                            detectorData.Invalid + "\",\"" + detectorData.FaultCode + "\");";

                    if(stringHashSet.add(arrayElement))// Able to add to the hash set
                    {// Copy the unique item
                        stringPasadena.add(sql);
                    }
                }
                // Insert the strings to database
                insertSQLBatch(psPasadena, stringPasadena,1000);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void insertPasadenaInventoryToDataBase(List<loadPasadenaData.DetectorInventory> detectorInventoryList){
        // This function is used to insert pasadena data to database

        try {
            Connection conPasadena = DriverManager.getConnection(MainFunction.hostPasadena,
                    MainFunction.cBlock.userName, MainFunction.cBlock.password);
            System.out.println("Succefully connect to the database!");

            String Header="insert into pasadena_data.detector_Inventory";
            if(detectorInventoryList.size()>0){// Not empty
                String sql;
                String arrayElement;
                List<String> stringPasadena= new ArrayList<String>();
                Statement psPasadena = conPasadena.createStatement();

                HashSet<String> stringHashSet= new HashSet<String>();; // Create the hash set
                for (int i=0;i<detectorInventoryList.size();i++) // Loop for each row
                {
                    loadPasadenaData.DetectorInventory detectorInventory=detectorInventoryList.get(i);
                    arrayElement=detectorInventory.IntersectionID + "-" + detectorInventory.DetectorID + "-" +
                            detectorInventory.Direction + "-" + detectorInventory.LastUpdateDate;
                    sql =  Header+ " values (\"" +
                            detectorInventory.IntersectionID + "\",\"" + detectorInventory.DetectorID + "\",\"" +
                            detectorInventory.Direction + "\",\"" + detectorInventory.LastUpdateDate + "\",\"" +
                            detectorInventory.Type + "\",\"" + detectorInventory.Lane + "\",\"" +
                            detectorInventory.MainStreet + "\",\"" + detectorInventory.CrossStreet + "\",\"" +
                            detectorInventory.Description+ "\");";
                    if(stringHashSet.add(arrayElement))// Able to add to the hash set
                    {// Copy the unique item
                        stringPasadena.add(sql);
                    }
                }
                // Insert the strings to database
                insertSQLBatch(psPasadena, stringPasadena,1000);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    
    public static List<Integer> getI210LACODetector(Connection con){
        // This function is used to get the I210 LACO detectors
        List<Integer> detectorList= new ArrayList<Integer>();
        try{
            Statement ps= con.createStatement();
            ResultSet resultSet=ps.executeQuery("Select DetectorID from i210_detector");
            while(resultSet.next()){
                detectorList.add(resultSet.getInt("DetectorID"));
            }
            return detectorList;
        }catch (SQLException e){
            e.printStackTrace();
            return detectorList;
        }
    }

    public static int checkDetectorExistence(List<Integer> detectorLACO,int devID){
        // This function is used to check the existence of a detector
        int exist=0;

        for (int i=0;i<detectorLACO.size();i++){
            if(devID==detectorLACO.get(i)){
                exist=1;
                break;
            }
        }
        return exist;
    }

    public static List<Integer> getI210LACOIntersection(Connection con){
        // This function is used to get the I210 LACO intersection signal IDs
        List<Integer> intersectionList= new ArrayList<Integer>();
        try{
            Statement ps= con.createStatement();
            ResultSet resultSet=ps.executeQuery("Select IntersectionID from i210_intersection_signal");
            while(resultSet.next()){
                intersectionList.add(resultSet.getInt("IntersectionID"));
            }
            return intersectionList;
        }catch (SQLException e){
            e.printStackTrace();
            return intersectionList;
        }
    }

    public static int checkIntersectionExistence(List<Integer> intersectionLACO,int intID){
        // This function is used to check the existence of an intersection
        int exist=0;

        for (int i=0;i<intersectionLACO.size();i++){
            if(intID==intersectionLACO.get(i)){
                exist=1;
                break;
            }
        }
        return exist;
    }

    public static boolean insertSQLBatch(Statement ps, List<String> string, int definedSize){
        // This function is used to insert SQL batch
        // definedSize: Can not set to high. Or else we will lose a lot of data
        int curSize=0;
        List<String> tmpString= new ArrayList<String>();
        try {
            for (int i = 0; i < string.size(); i++) {
                ps.addBatch(string.get(i));
                tmpString.add(string.get(i));
                curSize=curSize+1;
                if(curSize==definedSize || i==string.size()-1){
                    try {
                        ps.executeBatch();
                    }catch (SQLException e){
                        ps.clearBatch();
                        insertLineByLine(ps, tmpString);
                    }
                    curSize=0;
                    tmpString=new ArrayList<String>();
                    ps.clearBatch();
                }
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean insertLineByLine(Statement ps, List<String> string){
        // This function is used to insert line by line

        for (int i=0;i<string.size();i++){
            try{
                ps.execute(string.get(i));
            } catch (SQLException e) {
                System.out.println("Fail to insert: "+e.getMessage());
            }
        }
        return true;
    }

    public static String createSQLDetDataString(String header, int year, readIENData.DevData listDevData){
        // This is the function to create the sql detector data string
        String sql =  header+ year + " values (\"" +
                listDevData.devID + "\",\"" + listDevData.date + "\",\"" +
                listDevData.time + "\",\"" + listDevData.state + "\",\"" +
                listDevData.speed + "\",\"" + listDevData.occupancy + "\",\"" +
                listDevData.volume + "\",\"" + listDevData.avgSpeed + "\",\"" +
                listDevData.avgOccupancy + "\",\"" + listDevData.avgVolume + "\");";
        return sql;
    }

    public static String createSQLDetInvString(String header,readIENData.DevInv listDevInv){
        // This is the function to create the sql detector inventory string
        String sql = header + " values (\"" +
                listDevInv.orgID + "\",\"" + listDevInv.devID + "\",\"" +
                listDevInv.date + "\",\"" + listDevInv.time + "\",\"" +
                listDevInv.description + "\",\"" + listDevInv.roadwayName + "\",\"" +
                listDevInv.crossStreet + "\",\"" + listDevInv.latitude + "\",\"" +
                listDevInv.longitude + "\",\"" + listDevInv.direction + "\",\"" +
                listDevInv.avgPeriod + "\",\"" + listDevInv.associatedIntID + "\");";
        return sql;
    }

    public static String createSQLIntSigInvString(String header,readIENData.IntSigInv listIntSigInv){
        // This is the function to create the sql intersection signal inventory string
        String sql = header + " values (\"" +
                listIntSigInv.orgID + "\",\"" + listIntSigInv.intID + "\",\"" +
                listIntSigInv.date + "\",\"" + listIntSigInv.time + "\",\"" +
                listIntSigInv.signalType + "\",\"" + listIntSigInv.description + "\",\"" +
                listIntSigInv.mainStreet + "\",\"" + listIntSigInv.crossStreet + "\",\"" +
                listIntSigInv.latitude + "\",\"" + listIntSigInv.longitude + "\");";
        return sql;
    }

    public  static String createSQLIntSigDataString(String header, int year, readIENData.IntSigData listIntSigData){
        // This is the function to create the sql intersection signal data string
        String sql = header + year + " values (\"" +
                listIntSigData.orgID + "\",\"" + listIntSigData.intID + "\",\"" +
                listIntSigData.date + "\",\"" + listIntSigData.time + "\",\"" +
                listIntSigData.commState + "\",\"" + listIntSigData.sigState + "\",\"" +
                listIntSigData.timingPlan + "\",\"" + listIntSigData.desiredCycleLength + "\",\"" +
                listIntSigData.desiredOffset + "\",\"" + listIntSigData.actualOffset + "\",\"" +
                listIntSigData.controlMode + "\");";
        return sql;
    }

    public static String createSQLPlanPhaseString(String header, int year, readIENData.PlanPhase listPlanPhase){
        // This is the function to create the sql plan phase data string
        String sql = header + year + " values (\"" +
                listPlanPhase.orgID + "\",\"" + listPlanPhase.intID + "\",\"" +
                listPlanPhase.date + "\",\"" + listPlanPhase.time + "\",\"" +
                listPlanPhase.phaseIDTime + "\");";
        return sql;
    }

    public static String createSQLLastCyclePhaseString(String header, int year, readIENData.LastCyclePhase listLastCyclePhase){
        // This is the function to create the sql last-cycle phase data string
        String sql = header + year + " values (\"" +
                listLastCyclePhase.orgID + "\",\"" + listLastCyclePhase.intID + "\",\"" +
                listLastCyclePhase.date + "\",\"" + listLastCyclePhase.time + "\",\"" +
                listLastCyclePhase.cycleLength + "\",\"" + listLastCyclePhase.phaseIDTime + "\");";
        return sql;
    }

    public static boolean insertAggregatedDetectorDataToDataBaseBatch(Connection con, List<dataAggregation.aggregatedDetectorData> aggregatedData){

        try {
            String sql;
            List<String> stringSQL = new ArrayList<String>();
            Statement ps = con.createStatement();
            for(int i=0;i<aggregatedData.size();i++) {
                int year = aggregatedData.get(i).Date / 10000;
                //System.out.println((i+1)+" and "+aggregatedData.get(i).Date+ " and "+ aggregatedData.get(i).DetectorID);
                sql = "insert into detector_aggregated_data_" + year + " values (\"" +
                        aggregatedData.get(i).DetectorID + "\",\"" + aggregatedData.get(i).Date + "\",\"" +
                        aggregatedData.get(i).Time + "\",\"" + aggregatedData.get(i).Speed + "\",\"" +
                        aggregatedData.get(i).Occupancy + "\",\"" + aggregatedData.get(i).Volume + "\");";
                stringSQL.add(sql);
            }

            if(stringSQL.size()>0){
                insertSQLBatch(ps, stringSQL,100);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static boolean insertIENDataToDataBaseBatch(Connection conArcadia,Connection conLACO, readIENData.IENData ienData){
        // This function is used to insert IEN data to database using sql batch

        // Device data
        if(ienData.listDevData.size()>0) {// If it is not empty
            try {
                // Get the LACO detectors
                List<Integer> detectorLACO=getI210LACODetector(conLACO);
                // Variables
                int indicator;
                String sql;
                String header;
                List<String> stringArcadia= new ArrayList<String>();
                List<String> stringLACO= new ArrayList<String>();
                Statement psArcadia = conArcadia.createStatement();
                Statement psLACO = conLACO.createStatement();

                // Create the statement
                for (int i = 0; i < ienData.listDevData.size(); i++) { // Loop for each line
                    int year = ienData.listDevData.get(i).date / 10000;
                    if (ienData.listDevData.get(i).orgID.equals("5:1")) {
                        //Arcadia
                        header = "insert into arcadia_ien_server_data.detector_raw_data_";
                        sql=createSQLDetDataString(header,year, ienData.listDevData.get(i));
                        stringArcadia.add(sql);
                    }else if (ienData.listDevData.get(i).orgID.equals("29:1")){
                        //LACO
                        header = "insert into laco_ien_server_data.detector_raw_data_";
                        indicator=checkDetectorExistence(detectorLACO,ienData.listDevData.get(i).devID);
                        if(indicator==1){
                            // Generate the sql command
                            sql=createSQLDetDataString(header,year, ienData.listDevData.get(i));
                            stringLACO.add(sql);
                        }
                    }else {
                        System.out.println("Unknown organization ID!");
                        return false;
                    }
                }
                // Insert Data
                if(stringArcadia.size()>0){
                    insertSQLBatch(psArcadia, stringArcadia,100);
                }
                if(stringLACO.size()>0){
                    insertSQLBatch(psLACO, stringLACO,100);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Device Inventory
        if(ienData.listDevInv.size()>0) { // If it is not empty
            try {
                // Get the LACO detectors
                List<Integer> detectorLACO=getI210LACODetector(conLACO);
                // Variables
                int indicator;
                String sql;
                String header;
                List<String> stringArcadia= new ArrayList<String>();
                List<String> stringLACO= new ArrayList<String>();
                Statement psArcadia = conArcadia.createStatement();
                Statement psLACO = conLACO.createStatement();

                for (int i = 0; i < ienData.listDevInv.size(); i++) { // Loop for each line
                    if (ienData.listDevInv.get(i).orgID.equals("5:1")) {
                        //Arcadia
                        header = "insert into arcadia_ien_server_data.detector_inventory_ien";
                        sql=createSQLDetInvString(header,ienData.listDevInv.get(i));
                        stringArcadia.add(sql);
                    }else if (ienData.listDevInv.get(i).orgID.equals("29:1")){
                        //LACO
                        header = "insert into laco_ien_server_data.detector_inventory_ien";
                        indicator=checkDetectorExistence(detectorLACO,ienData.listDevInv.get(i).devID);
                        if(indicator==1){
                            // Generate the sql command
                            sql=createSQLDetInvString(header,ienData.listDevInv.get(i));
                            stringLACO.add(sql);
                        }
                    }else {
                        System.out.println("Unknown organization ID!");
                        return false;
                    }
                }
                // Insert Data
                if(stringArcadia.size()>0){
                    insertSQLBatch(psArcadia, stringArcadia,100);
                }
                if(stringLACO.size()>0){
                    insertSQLBatch(psLACO, stringLACO,100);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Intersection Signal Inventory
        if(ienData.listIntSigInv.size()>0) {
            try {
                // Get the LACO intersections
                List<Integer> intersectionLACO=getI210LACOIntersection(conLACO);
                // Create the statement
                int indicator;
                String sql;
                String header;
                List<String> stringArcadia= new ArrayList<String>();
                List<String> stringLACO= new ArrayList<String>();
                Statement psArcadia = conArcadia.createStatement();
                Statement psLACO = conLACO.createStatement();
                for (int i = 0; i < ienData.listIntSigInv.size(); i++) {
                    if (ienData.listIntSigInv.get(i).orgID.equals("5:1")) {
                        //Arcadia
                        header = "insert into arcadia_ien_server_data.intersection_signal_inventory";
                        sql=createSQLIntSigInvString(header,ienData.listIntSigInv.get(i));
                        stringArcadia.add(sql);
                    }else if (ienData.listIntSigInv.get(i).orgID.equals("29:1")){
                        //LACO
                        header = "insert into laco_ien_server_data.intersection_signal_inventory";
                        indicator=checkIntersectionExistence(intersectionLACO,ienData.listIntSigInv.get(i).intID);
                        if(indicator==1){
                            // Generate the sql command
                            sql=createSQLIntSigInvString(header,ienData.listIntSigInv.get(i));
                            stringLACO.add(sql);
                        }
                    }else {
                        System.out.println("Unknown organization ID!");
                        return false;
                    }
                }
                // Insert Data
                if(stringArcadia.size()>0){
                    insertSQLBatch(psArcadia, stringArcadia,100);
                }
                if(stringLACO.size()>0){
                    insertSQLBatch(psLACO, stringLACO,100);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Intersection Signal Data
        if(ienData.listIntSigData.size()>0) {
            try {
                // Get the LACO intersections
                List<Integer> intersectionLACO=getI210LACOIntersection(conLACO);
                int indicator=0;

                String sql;
                String header;
                List<String> stringArcadia= new ArrayList<String>();
                List<String> stringLACO= new ArrayList<String>();
                Statement psArcadia = conArcadia.createStatement();
                Statement psLACO = conLACO.createStatement();
                for (int i = 0; i < ienData.listIntSigData.size(); i++) {
                    int year = ienData.listIntSigData.get(i).date / 10000;
                    if (ienData.listIntSigData.get(i).orgID.equals("5:1")) {
                        //Arcadia
                        header = "insert into arcadia_ien_server_data.intersection_signal_data_";
                        sql=createSQLIntSigDataString(header,year,ienData.listIntSigData.get(i));
                        stringArcadia.add(sql);
                    }else if (ienData.listIntSigData.get(i).orgID.equals("29:1")){
                        //LACO
                        header = "insert into laco_ien_server_data.intersection_signal_data_";
                        indicator=checkIntersectionExistence(intersectionLACO,ienData.listIntSigData.get(i).intID);
                        if(indicator==1){
                            // Generate the sql command
                            sql=createSQLIntSigDataString(header,year,ienData.listIntSigData.get(i));
                            stringLACO.add(sql);
                        }
                    }else {
                        System.out.println("Unknown organization ID!");
                        return false;
                    }
                }
                // Insert Data
                if(stringArcadia.size()>0){
                    insertSQLBatch(psArcadia, stringArcadia,100);
                }
                if(stringLACO.size()>0){
                    insertSQLBatch(psLACO, stringLACO,100);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Plan Phase Setting
        if(ienData.listPlanPhase.size()>0) {
            try {
                // Get the LACO intersections
                List<Integer> intersectionLACO=getI210LACOIntersection(conLACO);
                // Variables
                int indicator;
                String sql;
                String header;
                List<String> stringArcadia= new ArrayList<String>();
                List<String> stringLACO= new ArrayList<String>();
                Statement psArcadia = conArcadia.createStatement();
                Statement psLACO = conLACO.createStatement();
                for (int i = 0; i < ienData.listPlanPhase.size(); i++) {
                    int year = ienData.listPlanPhase.get(i).date / 10000;
                    if (ienData.listPlanPhase.get(i).orgID.equals("5:1")) {
                        //Arcadia
                        header = "insert into arcadia_ien_server_data.plan_phase_";
                        sql=createSQLPlanPhaseString(header,year,ienData.listPlanPhase.get(i));
                        stringArcadia.add(sql);
                    }else if (ienData.listPlanPhase.get(i).orgID.equals("29:1")){
                        //LACO
                        header = "insert into laco_ien_server_data.plan_phase_";
                        indicator=checkIntersectionExistence(intersectionLACO,ienData.listPlanPhase.get(i).intID);
                        if(indicator==1){
                            // Generate the sql command
                            sql=createSQLPlanPhaseString(header,year,ienData.listPlanPhase.get(i));
                            stringLACO.add(sql);
                        }
                    }else {
                        System.out.println("Unknown organization ID!");
                        return false;
                    }
                }
                // Insert Data
                if(stringArcadia.size()>0){
                    insertSQLBatch(psArcadia, stringArcadia,100);
                }
                if(stringLACO.size()>0){
                    insertSQLBatch(psLACO, stringLACO,100);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Last-cycle Phase Setting
        if(ienData.listLastCyclePhase.size()>0) {
            try {
                // Get the LACO intersections
                List<Integer> intersectionLACO=getI210LACOIntersection(conLACO);
                // Variable
                int indicator;
                String sql;
                String header;
                List<String> stringArcadia= new ArrayList<String>();
                List<String> stringLACO= new ArrayList<String>();
                Statement psArcadia = conArcadia.createStatement();
                Statement psLACO = conLACO.createStatement();
                for (int i = 0; i < ienData.listLastCyclePhase.size(); i++) {
                    int year = ienData.listLastCyclePhase.get(i).date / 10000;
                    if (ienData.listLastCyclePhase.get(i).orgID.equals("5:1")) {
                        //Arcadia
                        header = "insert into arcadia_ien_server_data.last_cycle_phase_";
                        sql=createSQLLastCyclePhaseString(header,year,ienData.listLastCyclePhase.get(i));
                        stringArcadia.add(sql);
                    }else if (ienData.listLastCyclePhase.get(i).orgID.equals("29:1")){
                        //LACO
                        header = "insert into laco_ien_server_data.last_cycle_phase_";
                        indicator=checkIntersectionExistence(intersectionLACO,ienData.listLastCyclePhase.get(i).intID);
                        if(indicator==1){
                            // Generate the sql command
                            sql=createSQLLastCyclePhaseString(header,year,ienData.listLastCyclePhase.get(i));
                            stringLACO.add(sql);
                        }
                    }else {
                        System.out.println("Unknown organization ID!");
                        return false;
                    }
                }
                // Insert Data
                if(stringArcadia.size()>0){
                    insertSQLBatch(psArcadia, stringArcadia,100);
                }
                if(stringLACO.size()>0){
                    insertSQLBatch(psLACO, stringLACO,100);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    public static boolean insertDetectorConfigToDataBase (String host,String userName, String password, String type,
                                                         String configDir, String configName ){
        // This function is used to insert detector configuration into database

        // Get the connection and the correct database name
        Connection con = null;
        try {
            con = DriverManager.getConnection(host, userName, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Succefully connect to the database!");
        String databaseName;
        // Different databases have the same table called "detector_inventory"
        if(type.equals("TCSServer")) {
            databaseName = "arcadia_tcs_server_data";
        }else if(type.equals("IENArcadia")){
            databaseName = "arcadia_ien_server_data";
        }else if(type.equals("IENLACO")){
            databaseName = "laco_ien_server_data";
        }else{
            System.out.println("Unknown database type!");
            return false;
        }

        // Get the list of detector configuration
        List detConfigList;
        detConfigList=detectorConfig.loadConfig(configDir, configName);

        // Generate the SQL statements

        for (int i = 0; i < detConfigList.size(); i++) {
            detectorConfig.DetectorProperty tmpProperty = (detectorConfig.DetectorProperty) detConfigList.get(i);

            String sql = "insert into " + databaseName + ".detector_inventory values (\"" + tmpProperty.IntName + "\",\"" + tmpProperty.IntID +
                    "\",\"" + tmpProperty.County + "\",\"" + tmpProperty.City + "\",\"" + tmpProperty.RoadName + "\",\"" + tmpProperty.Direction
                    + "\",\"" + tmpProperty.SensorID + "\",\"" + tmpProperty.Movement + "\",\"" + tmpProperty.Status + "\",\"" + tmpProperty.DetourRoute
                    + "\",\"" + tmpProperty.Length + "\",\"" + tmpProperty.DistanceToStopbar + "\",\"" + tmpProperty.NumOfLanes + "\");";
            try{
                Statement ps = con.createStatement();
                ps.execute(sql);
            }catch (SQLException e){
                System.out.println("Fail to insert:"+sql);
            }
        }
        return true;
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
                insertSQLBatch(ps, string,100);
            }
            return true;
        } catch (SQLException e) {
            //If fail
            System.out.println("Fail to insert the data!");
            return false;
        }
    }

    public static boolean insertRawTCSDataToDataBase(Connection con, File fileName){
        // This function is used to insert raw TCS data to the database
        List<loadTCSDetectorData.DetectorRawDataProfile> detRawDataList;
        detRawDataList=loadTCSDetectorData.loadTCSServerCSVData(fileName);
        try {
            // Create a Statement from the connection
            Statement ps=con.createStatement();
            List<String> string= new ArrayList<String>();
            for(int i=0;i<detRawDataList.size();i++) {

                int year=detRawDataList.get(i).Date/10000;
                String sql = "insert into detector_data_raw_"+year+" values ("+detRawDataList.get(i).DetectorID+","+detRawDataList.get(i).Date+
                        ","+detRawDataList.get(i).Time+","+detRawDataList.get(i).Volume+","+detRawDataList.get(i).Occupancy+","+detRawDataList.get(i).Speed
                        +","+detRawDataList.get(i).Delay+","+detRawDataList.get(i).Stops+","+detRawDataList.get(i).S_Volume+","+detRawDataList.get(i).S_Occupancy
                        +","+detRawDataList.get(i).S_Speed+","+detRawDataList.get(i).S_Delay+","+detRawDataList.get(i).S_Stops+");";
                string.add(sql);
            }
            long start = System.currentTimeMillis();
            if(string.size()>0){
                insertSQLBatch(ps, string,100);
            }
            long end = System.currentTimeMillis();
            System.out.println("Total time taken (Insert) = " + (end - start) + " ms");
            ps.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean insertTCSPhaseTimeDataToDataBase(Connection con, List<loadTCSSignalData.PhaseTimes> phaseTimesList){
        // This function is used to insert TCS phase time data to the database
        try {
            // Create a Statement from the connection
            Statement ps=con.createStatement();
            List<String> string= new ArrayList<String>();

            for(int i=0;i<phaseTimesList.size();i++) {
                int year=phaseTimesList.get(i).EndDate/10000;
                String sql = "insert into phase_time_"+year+" values (\""+phaseTimesList.get(i).IntersectionID+
                        "\",\""+phaseTimesList.get(i).PlanID+"\",\""+phaseTimesList.get(i).EndDate+
                        "\",\""+phaseTimesList.get(i).EndTime+"\",\""+phaseTimesList.get(i).TimingStatus+"\",\""+phaseTimesList.get(i).CommFailures+
                        "\",\""+phaseTimesList.get(i).OperationFailures +"\",\""+phaseTimesList.get(i).DesiredCycleLength+
                        "\",\""+phaseTimesList.get(i).ActualCycleLength+"\",\""+phaseTimesList.get(i).DesiredOffset+"\",\""+phaseTimesList.get(i).ActualOffset+
                        "\",\""+phaseTimesList.get(i).PhaseMaxGreenTime+"\",\""+phaseTimesList.get(i).PhaseActualGreenTime+
                        "\",\""+phaseTimesList.get(i).PhasePedCalls+"\",\""+phaseTimesList.get(i).PhaseIsCoordinated+"\");";
                string.add(sql);
            }
            long start = System.currentTimeMillis();
            if(string.size()>0){
                insertSQLBatch(ps, string,1000);
            }
            long end = System.currentTimeMillis();
            System.out.println("Total time taken (Insert Phase Times) = " + (end - start) + " ms");
            ps.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean insertTCSTimingPlansDataToDataBase(Connection con, List<loadTCSSignalData.TimingPlans> timingPlansList){
        // This function is used to insert TCS phase time data to the database
        try {
            // Create a Statement from the connection
            Statement ps=con.createStatement();
            List<String> string= new ArrayList<String>();

            for(int i=0;i<timingPlansList.size();i++) {
                int year=timingPlansList.get(i).EndDate/10000;
                String sql = "insert into timing_plan_"+year+" values (\""+timingPlansList.get(i).IntersectionID+
                        "\",\""+timingPlansList.get(i).PlanID+"\",\""+timingPlansList.get(i).StartDate+
                        "\",\""+timingPlansList.get(i).StartTime+"\",\""+timingPlansList.get(i).EndDate+"\",\""+timingPlansList.get(i).EndTime+
                        "\",\""+timingPlansList.get(i).PhaseAverageGreenTimePercent +"\",\""+timingPlansList.get(i).PhaseInstanceMaxGreen+
                        "\",\""+timingPlansList.get(i).PhasePedCalls+"\");";
                string.add(sql);
            }
            long start = System.currentTimeMillis();
            if(string.size()>0){
                insertSQLBatch(ps, string,1000);
            }
            long end = System.currentTimeMillis();
            System.out.println("Total time taken (Insert Timing Plans) = " + (end - start) + " ms");
            ps.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
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

    public static boolean insertHealthFileToDataBase(Connection con, File fileName){
        // This function is used to insert health data into database

        if(!fileName.exists()) // Check the existence of file
        { // If not
            System.out.println("Can not find the configuration file!");
            return false;
        }
        else{//If yes
            try {
                FileReader fr= new FileReader(fileName);
                BufferedReader br = new BufferedReader(fr);
                String line;

                Statement ps=con.createStatement();
                List<String> string= new ArrayList<String>();
                String sql;
                long start = System.currentTimeMillis();
                line = br.readLine(); //Ignore the first line: header
                while ((line = br.readLine()) != null) { // Loop for the following rows

                    String [] healthRow= line.split(",");
                    sql="insert into detector_health values ("+
                            healthRow[0]+","+healthRow[1]+","+healthRow[2]+","+
                            healthRow[3]+","+healthRow[4]+","+healthRow[5]+","+
                            healthRow[6]+","+healthRow[7]+","+healthRow[8]+","+healthRow[9]+");";
                    string.add(sql);
                }
                br.close();
                fr.close();
                long end = System.currentTimeMillis();
                System.out.println("Total time taken (Read) = " + (end - start) + " ms");

                start = System.currentTimeMillis();
                if(string.size()>0){
                    insertSQLBatch(ps, string,100);
                }
                end = System.currentTimeMillis();
                System.out.println("Total time taken (Insert) = " + (end - start) + " ms");
                ps.close();
                return true;
            }catch (IOException e){
                e.printStackTrace();
                return false;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
