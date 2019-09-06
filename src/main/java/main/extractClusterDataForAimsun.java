package main;

import config.detectorConfig;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import extractData.getDataFromDatabase.ClusterDetectorData;
import config.detectorConfig.DetectorProperty;

import static extractData.extractDataToFile.outputClusterDataToFile;
import static extractData.getDataFromDatabase.getClusteredDataForAllDetectorsForGivenTime;

public class extractClusterDataForAimsun {

    public static void main(String [] args) throws IOException {

        // Static Variables
        String host="jdbc:mysql://localhost:3306/arcadia_tcs_server_data?useSSL=false"; // For TCS server
        String userName="root";
        String password="!Ganqijian2017";

        try {
            Connection con = DriverManager.getConnection(host, userName, password);
            System.out.println("Succefully connect to the database!");

            // Configurations (changeable if needed)
            String dataTableName = "detector_data_processed_2019";
            String healthTableName = "detector_health";
            int year = 2019;
            boolean useMedian = true;
            int interval = 300;// N*3600 seconds
            String organization = "Arcadia";
            String[] clusterSettings = new String[]{"All", "Weekday", "Weekend", "Monday", "Tuesday", "Wednesday",
                    "Thursday", "Friday", "Saturday", "Sunday"};
            String outputPath = "L:\\arterial_detector_analysis_files\\clusterData\\";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = simpleDateFormat.parse("2019-01-01");

            // Get the list of detector configuration
            List<DetectorProperty> detConfigList = detectorConfig.readDetectorConfigFromDataBase(con, organization, startDate);

            for (int s = 0; s < clusterSettings.length; s++) {
                File fileName = new File(outputPath + "Aimsun_Data_Input_" + organization + "_" + year + "_" + clusterSettings[s] + ".csv");
                fileName.delete();
                String clusterSetting = clusterSettings[s];
                System.out.println("Thread Running for " + clusterSetting);
                // Loop for each time step
                int totalStep = 24 * 3600 / interval;
                for (int i = 0; i < totalStep; i++) {
                    int timeStamp = interval * i;
                    List<ClusterDetectorData> clusterDetectorDataListByTime= getClusteredDataForAllDetectorsForGivenTime
                            (con, dataTableName, healthTableName, detConfigList, year, useMedian, timeStamp, interval, clusterSetting);
                    if(clusterDetectorDataListByTime.size()>0) {
                        outputClusterDataToFile(i,clusterDetectorDataListByTime, fileName);
                    }
                    clusterDetectorDataListByTime.clear();
                    System.gc();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }




}
