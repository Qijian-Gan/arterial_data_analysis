package main;

/**
 * Created by Qijian-Gan on 9/23/2017.
 */

import analysis.dataFiltering;
import analysis.healthAnalysis;
import analysis.dataAggregation;
import loadData.loadPasadenaData;
import loadData.readIENData;
import saveData.saveDataToDatabase;
import config.detectorConfig;
import config.loadProgramSettingsFromFile;
import config.getTaskID;
import extractData.extractDataToFile;
import networkConnection.networkConnection;
import networkConnection.realTimeDataFeed;
import networkConnection.automaticDetectorHealthAnalysis;
import Utility.util;
import settings.programSettings;


//import javafx.concurrent.Task;
import java.io.File;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


public class MainFunction{

    // ***********Global settings************
    // Database
    public static String host="jdbc:mysql://localhost:3306/arcadia_tcs_server_data?useSSL=false"; // For TCS server
    public static String hostPasadena="jdbc:mysql://localhost:3306/pasadena_data";  // For Pasadena data
    public static String hostIENArcadia="jdbc:mysql://localhost:3306/arcadia_ien_server_data"; // For IEN-Arcadia
    public static String hostIENLACO="jdbc:mysql://localhost:3306/laco_ien_server_data";  // For IEN-LACO
    // Users
    public static String userName="root";
    public static String password="!Ganqijian2017";
    // VPN connection
    public static String vpnServerName="Arcadia";
    public static String vpnUser="path";
    public static String vpnPassword="arcadia";
    public static String serverIP="172.16.128.71";
    public static String detectorFolder="D$\\TCSSupport\\DetectorArchive";
    public static String outputFolder="L:\\tmpData";
    public static String storeFolder="L:\\DetectorData";

    // Variables
    public static Connection conTCSServer;
    public static Connection conPasadena;
    public static Connection conArcadia;
    public static Connection conLACO;

    public static programSettings cBlock=new programSettings();
    // ***********Global settings************

    public static void main(String [] args){

        // Selection of type of tasks

        //int taskID=Integer.parseInt(args[0].trim());
        //int taskID=10;
        //taskID=getTaskID.getTaskIDFromScreen();
        int taskID=0;
        if(args.length>0){
            taskID=Integer.parseInt(args[0].trim());
        }else{
            taskID=getTaskID.getTaskIDFromScreen();
        }

        // Get the program settings
        File configFile = new File("");
        configFile=new File(configFile.getAbsolutePath(),"\\src\\main\\java\\arterialAnalysis.conf");
        System.out.println("Current configuration file path : "+configFile.getAbsolutePath());
        cBlock=loadProgramSettingsFromFile.loadProgramSettings(configFile.getAbsolutePath());

        // Check the selected task
        if(taskID==1){
            System.out.print("1:  Insert Detector Config To DataBase\n"); // Load configuration files
            if(cBlock.configDirArcadia !=null && cBlock.configNameArcadia !=null){
                String type="TCSServer";
                saveDataToDatabase.mainInsertDetConfigToDataBase(type);
                type="IENArcadia";
                saveDataToDatabase.mainInsertDetConfigToDataBase(type);
            }
            if(cBlock.configDirLACO !=null && cBlock.configNameLACO !=null){
                String type="IENLACO";
                saveDataToDatabase.mainInsertDetConfigToDataBase(type);
            }
        }else if(taskID==2){
            System.out.print("2:  Extract Health Results To File\n"); // Extract the health results
            if(cBlock.healthOutputFolder ==null || cBlock.dataSource ==null
                    || cBlock.organization ==null || cBlock.startDateString ==null || cBlock.endDateString ==null ){
                System.out.println("Lacking appropriate settings to extract detector health results!");
                System.exit(-1);
            }
            String hostName;
            if(cBlock.dataSource.equals("TCSServer")) {
                if(cBlock.organization.equals("Arcadia")){// TCS server only for Arcadia
                    String outputFileName="HealthResult_"+cBlock.organization+"_"+cBlock.dataSource+"_"+
                            cBlock.startDateString+"_"+cBlock.endDateString;
                    hostName = host;
                    String curOrganization="Arcadia";
                    extractDataToFile.mainExtractDataToFile(hostName, outputFileName,curOrganization);
                }else {
                    System.out.println("Unkown organization!");
                    System.exit(-1);
                }
            }else if(cBlock.dataSource.equals("IEN")){
                if(!(cBlock.organization.equals("Arcadia") ||cBlock.organization.equals("All")||
                        cBlock.organization.equals("LACO"))) {
                    System.out.println("Unkown organization!");
                    System.exit(-1);
                }
                if(cBlock.organization.equals("Arcadia") ||cBlock.organization.equals("All")) {
                    // Output Arcadia IEN health results
                    String outputFileName="HealthResult_Arcadia_"+cBlock.dataSource+"_"+
                            cBlock.startDateString+"_"+cBlock.endDateString;
                    hostName = hostIENArcadia;
                    String curOrganization="Arcadia";
                    extractDataToFile.mainExtractDataToFile(hostName, outputFileName,curOrganization);
                }
                if(cBlock.organization.equals("LACO")||cBlock.organization.equals("All")) {
                    // Output LACO IEn health results
                    String outputFileName="HealthResult_LACO_"+cBlock.dataSource+"_"+
                            cBlock.startDateString+"_"+cBlock.endDateString;
                    hostName = hostIENLACO;
                    String curOrganization="LACO";
                    extractDataToFile.mainExtractDataToFile(hostName, outputFileName,curOrganization);
                }
            }else if(cBlock.dataSource.equals("Pasadena")) {
                if(cBlock.organization.equals("Pasadena")){// TCS server only for Arcadia
                    String outputFileName="HealthResult_"+cBlock.organization+"_"+cBlock.dataSource+"_"+
                            cBlock.startDateString+"_"+cBlock.endDateString;
                    hostName = hostPasadena;
                    String curOrganization="Pasadena";
                    extractDataToFile.mainExtractDataToFile(hostName, outputFileName,curOrganization);
                }else {
                    System.out.println("Unkown organization!");
                    System.exit(-1);
                }
            }else {
                System.out.println("Unkown data source!");
                System.exit(-1);
            }
        }else if(taskID==3) {
            System.out.print("3:  Load Arcadia TCS Server Raw Data To DataBase\n"); // Load TCS raw data
            if (cBlock.rawTCSDataDir == null || cBlock.rawTCSDataNewDir == null) {
                System.out.println("Lacking appropriate settings to read the TCS server data!");
                System.exit(-1);
            }
            saveDataToDatabase.mainLoadTCSRawDataToDataBase();
        }else if(taskID==4){
            System.out.print("4:  Load Pasadena Data To DataBase\n"); // Load Pasadena data
            loadPasadenaData.mainPasadenaRead();

        }else if(taskID==5){
            System.out.print("5:  Load IEN Data To DataBase\n"); // Load IEN data
            if(cBlock.rawIENDataDir ==null || cBlock.rawIENDataNewDir ==null){
                System.out.println("Lacking appropriate settings to read the IEN data!");
                System.exit(-1);
            }
            readIENData.mainIENRead();

        }else if(taskID==6){
            System.out.print("6:  Aggregation Of The IEN Data\n"); // Data aggregation
            if(cBlock.method.equals("Manual")){
                // If it the manual process
                if(cBlock.interval ==0 || cBlock.startDateString ==null || cBlock.endDateString ==null){
                    System.out.println("Lacking appropriate aggregation settings!");
                    System.exit(-1);
                }
                dataAggregation.mainAggregationHistorical();
            }
        }else if(taskID==7){
            System.out.print("7:  Detector Health Analysis & Data Filtering And Imputation\n"); // Health analysis and data filtering
            if(cBlock.fromDate ==null || cBlock.toDate ==null
                    || cBlock.dataSourceHealth ==null || cBlock.organizationHealth ==null || cBlock.defaultInterval ==0 ){
                System.out.println("Lacking appropriate settings to perform detector health analysis and data filtering and imputation!");
                System.exit(-1);
            }
            if(cBlock.spanImputation ==0 || cBlock.useMedianOrNotImputation <0 ||
                    cBlock.methodSmoothing ==null || cBlock.spanSmoothing ==0){
                System.out.println("Lacking appropriate settings for data filtering!");
                System.exit(-1);
            }
            String hostName;
            if(cBlock.dataSourceHealth.equals("TCSServer")) {
                if(cBlock.missingRateThreshold_TCSServer ==0 || cBlock.maxZeroValueThreshold_TCSServer ==0
                        || cBlock.highValueRateThreshold_TCSServer ==0 || cBlock.highFlowValue_TCSServer ==0
                        || cBlock.inconsisRateWithoutSpeedThreshold_TCSServer ==0 ){
                    System.out.println("Lacking appropriate thresholds for detector health analysis!");
                    System.exit(-1);
                }
                if(cBlock.organizationHealth.equals("Arcadia")){// TCS server only for Arcadia
                    hostName = host;
                    healthAnalysis.mainHealthAnalysisAndDataFiltering(hostName,"Arcadia");
                }else {
                    System.out.println("Unkown organization!");
                    System.exit(-1);
                }
            }else if(cBlock.dataSourceHealth.equals("IEN")){
                if(cBlock.missingRateThreshold_IEN ==0 || cBlock.maxZeroValueThreshold_IEN ==0
                        || cBlock.highValueRateThreshold_IEN ==0 || cBlock.highFlowValue_IEN ==0
                        || cBlock.inconsisRateWithoutSpeedThreshold_IEN ==0 ){
                    System.out.println("Lacking appropriate thresholds for detector health analysis!");
                    System.exit(-1);
                }
                if(cBlock.organizationHealth.equals("Arcadia")) {
                    // Output Arcadia IEN health results
                    hostName = hostIENArcadia;
                    String curOrganization="Arcadia";
                    healthAnalysis.mainHealthAnalysisAndDataFiltering(hostName,curOrganization);
                }
                else if(cBlock.organizationHealth.equals("LACO")) {
                    // Output LACO IEn health results
                    hostName = hostIENLACO;
                    String curOrganization="LACO";
                    healthAnalysis.mainHealthAnalysisAndDataFiltering(hostName,curOrganization);
                }else if(cBlock.organizationHealth.equals("All")){
                    hostName = hostIENArcadia;
                    String curOrganization="Arcadia";
                    healthAnalysis.mainHealthAnalysisAndDataFiltering(hostName,curOrganization);

                    hostName = hostIENLACO;
                    curOrganization="LACO";
                    healthAnalysis.mainHealthAnalysisAndDataFiltering(hostName,curOrganization);
                }
                else {
                    System.out.println("Unkown organization!");
                    System.exit(-1);
                }
            }else if(cBlock.dataSourceHealth.equals("Pasadena")) {// Use the same thresholds as the data from TCS server
                if(cBlock.missingRateThreshold_TCSServer ==0 || cBlock.maxZeroValueThreshold_TCSServer ==0
                        || cBlock.highValueRateThreshold_TCSServer ==0 || cBlock.highFlowValue_TCSServer ==0
                        || cBlock.inconsisRateWithoutSpeedThreshold_TCSServer ==0 ){
                    System.out.println("Lacking appropriate thresholds for detector health analysis!");
                    System.exit(-1);
                }
                if(cBlock.organizationHealth.equals("Pasadena")){// For Pasadena data
                    hostName = hostPasadena;
                    healthAnalysis.mainHealthAnalysisAndDataFiltering(hostName,"Pasadena");
                }else {
                    System.out.println("Unkown organization!");
                    System.exit(-1);
                }
            }
            else {
                System.out.println("Unkown data source!");
                System.exit(-1);
            }
        }else if(taskID==8){
            System.out.print("8:  Load Health File(csv) To DataBase\n");
            if(cBlock.configDirHealth!=null){
                saveDataToDatabase.mainLoadHealthFileToDataBase();
            }
        }else if(taskID==9){
            System.out.print("9:  Load TCS Signal Phasing data To DataBase\n");
            saveDataToDatabase.mainLoadTCSSignalPhaseDataToDataBase();

        }else if(taskID==10){
            System.out.print("10:  Get real-time data from TCS server!\n");
            realTimeDataFeed.copyLatestDetectorFileFromTCSRemoteServer(vpnServerName,
                    serverIP, detectorFolder,vpnUser, vpnPassword,outputFolder);
            realTimeDataFeed.processAndUpdateRealTimeDetectorData(host,userName, password,outputFolder,storeFolder);
        }else if(taskID==11){
            System.out.print("11:  Perform automatic detector health analysis for Arcadia!\n");
            automaticDetectorHealthAnalysis.updateArcadiaTCSData();
        }
        else if(taskID==12){
            System.out.print("12:  Perform automatic aggregation and detector health analysis for LACO & Arcadia from IEN!\n");
            automaticDetectorHealthAnalysis.updateLACOArcadiaIENData();
        }
        else{
            System.out.println("Unknown task!");
            System.exit(-1);
        }
    }
}
