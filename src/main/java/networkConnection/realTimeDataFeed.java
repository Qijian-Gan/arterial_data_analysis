package networkConnection;

import loadData.loadTCSDetectorData;
import saveData.saveDataToDatabase;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by Qijian-Gan on 2/22/2018.
 */
public class realTimeDataFeed {
    // This is the class for real-time data feed

    //*********************Arcadia TCS server***********************
    public static void copyLatestDetectorFileFromTCSRemoteServer(String ServerName
            ,String IP, String FolderLocation,String User, String PW,String OutputFolder){
        // This function is used to copy latest detector files from TCS remote server

        // Delete the mapped disk & disconnect the VPN
        networkConnection.deleteMappedFolderFromRemoteServer("x");
        networkConnection.disconnectFromVPN(ServerName);
        // Connect to the VPN
        networkConnection.connectToVPN(ServerName,User,PW);
        // Wait for five seconds
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Map the remote disk
        networkConnection.mapDetectorFolderFromRemoteServer("x",IP, FolderLocation,User, PW);
        // Wait for five seconds
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Get the date for the previous Sunday
        DateFormat df=new SimpleDateFormat("yyyy-M-d");
        Calendar c=Calendar.getInstance();
        if(c.get(Calendar.DAY_OF_WEEK)==1 && c.get(Calendar.HOUR_OF_DAY)<3){
            // Provide some time buffer to make sure getting the complete data of the previous week
            int dayOfTheWeek = c.get( Calendar.DAY_OF_WEEK );
            c.add( Calendar.DAY_OF_WEEK, Calendar.SUNDAY - dayOfTheWeek-7 );
        }else{
            c.set(Calendar.DAY_OF_WEEK,Calendar.SUNDAY);
        }
        String DateSunday=df.format(c.getTime()).trim().toString();
        System.out.println("Retrieve data for Date="+DateSunday);

        // Construct the "command"
        String[] command = new String[6];
        command[0] = "cmd";
        command[1] = "/c";
        command[2] = "copy";
        command[3] = "/y";
        command[4] = "x:\\*"+DateSunday+".csv ";
        command[5] = OutputFolder;
        try {
            ProcessBuilder copyFiles = new ProcessBuilder(command);
            copyFiles.redirectErrorStream(true);
            Process p = copyFiles.start();
            // Avoid the "copy" function hanging
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null)
                System.out.println("Tasklist: " + line);
            p.waitFor();

        } catch (IOException e) {
            System.out.println("Fail to excute: " + command);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Delete the mapped disk
        networkConnection.deleteMappedFolderFromRemoteServer("x");
        // Disconnect the VPN
        networkConnection.disconnectFromVPN(ServerName);
    }

    public static void processAndUpdateRealTimeDetectorData(String Host, String User, String PW,
                                                            String OutputFolder,String StoreFolder){
        // This function is used to process and update real-time detector data

        // Get the list of files
        File fileDir = new File(OutputFolder);
        File[] listOfFiles = fileDir.listFiles();

        // Get the date for the previous Sunday
        // Get the date for the previous Sunday
        DateFormat df=new SimpleDateFormat("yyyy-M-d");
        Calendar c=Calendar.getInstance();
        if(c.get(Calendar.DAY_OF_WEEK)==1 && c.get(Calendar.HOUR_OF_DAY)<3){
            // Provide some time buffer to make sure getting the complete data of the previous week
            int dayOfTheWeek = c.get( Calendar.DAY_OF_WEEK );
            c.add( Calendar.DAY_OF_WEEK, Calendar.SUNDAY - dayOfTheWeek-7 );
        }else{
            c.set(Calendar.DAY_OF_WEEK,Calendar.SUNDAY);
        }
        String DateSunday=df.format(c.getTime()).trim().toString();

        // First, read the historical data and construct the hash set
        File HashSetFile=new File(OutputFolder,"HashSet_"+DateSunday+".txt");
        HashSet<String> hashSetStr=readHashSetFile(HashSetFile);
        List<loadTCSDetectorData.DetectorRawDataProfile> detRawDataList= new ArrayList<loadTCSDetectorData.DetectorRawDataProfile>();

        for(int i=0;i<listOfFiles.length;i++) {
            if(listOfFiles[i].getName().contains(DateSunday+".csv")){
                List<loadTCSDetectorData.DetectorRawDataProfile> tmpList=
                        loadTCSDetectorData.loadTCSServerCSVData(listOfFiles[i].getAbsoluteFile());
                for(int j=0;j<tmpList.size();j++){
                    String tmpStr=tmpList.get(j).DetectorID+"-"+tmpList.get(j).Date+"-"+tmpList.get(j).Time;
                    if(hashSetStr.add(tmpStr)){
                        // Only add the new data
                        detRawDataList.add(tmpList.get(j));
                    }
                }
            }
        }

        try {
            Connection con= DriverManager.getConnection(Host,User, PW);
            String TableName="detector_data_raw";
            // Update the real-time data
            updateRawTCSDataToDataBase(con, TableName, detRawDataList);
            // Save the hashset file
            saveHashSetFile(hashSetStr,HashSetFile);
            //Move all existing files
            moveExistingDataFiles(OutputFolder,StoreFolder);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static boolean updateRawTCSDataToDataBase(Connection con, String tableName
            , List<loadTCSDetectorData.DetectorRawDataProfile> detRawDataList){
        // This function is used to insert raw TCS data to the database

        try {
            // Create a Statement from the connection
            Statement ps=con.createStatement();
            List<String> string= new ArrayList<String>();
            for(int i=0;i<detRawDataList.size();i++) {

                int year=detRawDataList.get(i).Date/10000;
                String sql = "insert into "+tableName+"_"+year+" values ("+detRawDataList.get(i).DetectorID+","+detRawDataList.get(i).Date+
                        ","+detRawDataList.get(i).Time+","+detRawDataList.get(i).Volume+","+detRawDataList.get(i).Occupancy+","+detRawDataList.get(i).Speed
                        +","+detRawDataList.get(i).Delay+","+detRawDataList.get(i).Stops+","+detRawDataList.get(i).S_Volume+","+detRawDataList.get(i).S_Occupancy
                        +","+detRawDataList.get(i).S_Speed+","+detRawDataList.get(i).S_Delay+","+detRawDataList.get(i).S_Stops+");";
                string.add(sql);
            }
            long start = System.currentTimeMillis();
            if(string.size()>0){
                saveDataToDatabase.insertSQLBatch(ps, string,5000);
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

    public static HashSet<String> readHashSetFile(File HashSetFile){
        // This function is used to read the hash set file
        HashSet<String> hashSetStr=new HashSet<String>();
        if(HashSetFile.exists()){
            try {
                FileReader fr = new FileReader(HashSetFile);
                BufferedReader br = new BufferedReader(fr);
                String line;
                while ((line = br.readLine()) != null) {
                    String[] tmpStr=line.split(",");
                    for(int i=0;i<tmpStr.length;i++){
                        hashSetStr.add(tmpStr[i].trim());
                    }
                }
                br.close();
                fr.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return hashSetStr;
    }

    public static boolean saveHashSetFile(HashSet<String> hashSetStr,File HashSetFile){
        // This function is used to save hash set file

        if(HashSetFile.exists()){
            HashSetFile.delete();
        }
        try {
            FileWriter fw = new FileWriter(HashSetFile);
            BufferedWriter bw = new BufferedWriter(fw);
            Iterator iterator=hashSetStr.iterator();
            while(iterator.hasNext()){
                bw.write(iterator.next().toString()+",");
            }
            bw.close();
            fw.close();
            return true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void moveExistingDataFiles(String OutputFolder,String StoreFolder){
        // This function is used to move the files in OutputFolder to StoreFolder

        try {
            String[] command = new String[6];
            command[0] = "cmd";
            command[1] = "/c";
            command[2] = "move";
            command[3] ="/y";
            command[4] = OutputFolder+"\\DetectorArchive* ";
            command[5] = StoreFolder;
            ProcessBuilder copyFiles = new ProcessBuilder(command);
            copyFiles.redirectErrorStream(true);
            Process p = copyFiles.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null)
                System.out.println("Tasklist: " + line);
            p.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
