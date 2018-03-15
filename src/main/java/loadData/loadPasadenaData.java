package loadData;

import Utility.util;
import main.MainFunction;
import saveData.saveDataToDatabase;

import java.util.ArrayList;
import java.io.*;
import java.util.*;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Qijian-Gan on 11/8/2017.
 */
public class loadPasadenaData {
    // This function is used to load pasadena data

    public static void mainPasadenaRead(){
        // Get the list of files
        File fileDir = new File(MainFunction.cBlock.rawPasadenaDataDir);
        File[] listOfFiles = fileDir.listFiles();
        try{
            // Create the connections to the databases
            MainFunction.conPasadena = DriverManager.getConnection(MainFunction.hostPasadena,
                    MainFunction.userName, MainFunction.password);
            System.out.println("Succefully connect to the database!");

            for (int i = 0; i < listOfFiles.length; i++) {// Loop for each file
                if (listOfFiles[i].isFile()) {
                    // If it is a file
                    String DataFileName = MainFunction.cBlock.rawPasadenaDataDir + "\\" + listOfFiles[i].getName();
                    long startTime = System.currentTimeMillis();
                    PasadenaData pasadenaData=readPasadenaData(DataFileName);
                    long endTime = System.currentTimeMillis();
                    System.out.println("Process Time(ms):" + (endTime-startTime)+
                            " & Rows Processed:"+pasadenaData.detectorDataList.size());

                    startTime = System.currentTimeMillis();
                    saveDataToDatabase.insertPasadenaDataToDataBase(pasadenaData.detectorDataList);
                    endTime = System.currentTimeMillis();
                    System.out.println("Time for inserting data to database(ms):" + (endTime-startTime));

                    startTime = System.currentTimeMillis();
                    saveDataToDatabase.insertPasadenaInventoryToDataBase(pasadenaData.detectorInventoryList);
                    endTime = System.currentTimeMillis();
                    System.out.println("Time for inserting Inventory to database(ms):" + (endTime-startTime));
                }
            }
            // Remove the files
            for (int j=0;j<listOfFiles.length;j++) {
                util.moveFileFromAToB(MainFunction.cBlock.rawPasadenaDataDir, MainFunction.cBlock.rawPasadenaDataNewDir,
                        listOfFiles[j].getName());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static class DetectorInventory{
        // This is the class of Pasadena's detector inventory
        public DetectorInventory(int _IntersectionID, int _DetectorID, String _Direction,int _LastUpdateDate,
                                 String _Type, String _Lane, String _MainStreet,
                                 String _CrossStreet, String _Description){
            this.IntersectionID=_IntersectionID;
            this.DetectorID=_DetectorID;
            this.Direction=_Direction;
            this.LastUpdateDate=_LastUpdateDate;
            this.Type=_Type;
            this.Lane=_Lane;
            this.MainStreet=_MainStreet;
            this.CrossStreet=_CrossStreet;
            this.Description=_Description;
        }
        public  int IntersectionID;
        public  int DetectorID;
        public  String Direction;
        public  int LastUpdateDate;
        public  String Type;
        public  String Lane;
        public  String MainStreet;
        public  String CrossStreet;
        public  String Description;
    }

    public static class DetectorData{
        // This is the class of Pasadena's detector data
        public DetectorData(int _DetectorID, int _IntersectionID, int _Date, int _Time, int _Period, double _Volume,
                            double _Occupancy, double _Speed, int _Invalid, int _FaultCode){
            this.DetectorID=_DetectorID;
            this.IntersectionID=_IntersectionID;
            this.Date=_Date;
            this.Time=_Time;
            this.Period=_Period;
            this.Volume=_Volume;
            this.Occupancy=_Occupancy;
            this.Speed=_Speed;
            this.Invalid=_Invalid;
            this.FaultCode=_FaultCode;
        }
        public  int DetectorID;
        public  int IntersectionID;
        public  int Date;
        public  int Time;
        public  int Period;
        public  double Volume;
        public  double Occupancy;
        public  double Speed;
        public  int Invalid;
        public  int FaultCode;
    }

    public static class IntersectionInf{
        public IntersectionInf(int _IntersectionID, String _MainStreet, String _CrossStreet){
            this.IntersectionID=_IntersectionID;
            this.MainStreet=_MainStreet;
            this.CrossStreet=_CrossStreet;
        }
        public  int IntersectionID;
        public  String MainStreet;
        public  String CrossStreet;
    }

    public static class DateTimeAttributes{
        public DateTimeAttributes(int _Date, int _Time){
            this.Date=_Date;
            this.Time=_Time;
        }
        public  int Date;
        public  int Time;
    }

    public static class DetectorNameAttributes{
        public DetectorNameAttributes(String _Lane, String _Direction,String _Type,String _Description){
            this.Lane=_Lane;
            this.Direction=_Direction;
            this.Type=_Type;
            this.Description=_Description;
        }
        public  String Lane;
        public  String Direction;
        public  String Type;
        public  String Description;
    }

    public static class PasadenaData{
        public PasadenaData(List<DetectorInventory> _detectorInventoryList, List<DetectorData> _detectorDataList){
            this.detectorDataList=_detectorDataList;
            this.detectorInventoryList=_detectorInventoryList;
        }
        public  List<DetectorInventory> detectorInventoryList;
        public  List<DetectorData> detectorDataList;
    }

    public static PasadenaData readPasadenaData(String fileName){
        // This function is used to read Pasadena data

        File inputFile = new File(fileName);
        if(!inputFile.exists())
        {
            System.out.println("Can not find the detector file!");
            return null;
        }
        else{
            System.out.println("Loading:"+inputFile);
        }
        String line=null;
        try {
            List<DetectorInventory> detectorInventoryList=new ArrayList<DetectorInventory>();
            List<DetectorData> detectorDataList=new ArrayList<DetectorData>();

            FileReader fr= new FileReader(inputFile);
            BufferedReader br = new BufferedReader(fr);
            line = br.readLine(); //Ignore the first line: header
            //line = br.readLine(); //Ignore the second line: header

            while ((line = br.readLine()) != null && !line.equals("")) { // Loop for the following rows
                String[] tmpdataRow = line.split(",");
                List<String> dataRow = new ArrayList<String>();
                for (String tmpRow: tmpdataRow) {
                    if (!tmpRow.equals("")) {
                        dataRow.add(tmpRow);
                    }
                }
                int FaultCode = Integer.parseInt(dataRow.get(dataRow.size() - 1).trim());
                int Invalid = Integer.parseInt(dataRow.get(dataRow.size() - 2).trim());

                double Speed, Occupancy, Volume;
                if (dataRow.get(dataRow.size() - 3).trim().equals("NULL")) {
                    Speed = 0;
                } else {
                    Speed = Double.parseDouble(dataRow.get(dataRow.size() - 3).trim());
                }
                if (dataRow.get(dataRow.size() - 4).trim().equals("NULL")) {
                    Occupancy = 0;
                } else {
                    Occupancy = Double.parseDouble(dataRow.get(dataRow.size() - 4).trim());
                }
                if (dataRow.get(dataRow.size() - 5).trim().equals("NULL")) {
                    Volume = 0;
                } else {
                    Volume = Double.parseDouble(dataRow.get(dataRow.size() - 5).trim());
                }
                int Period = Integer.parseInt(dataRow.get(dataRow.size() - 6).trim());

                String IntersectionInformation = dataRow.get(dataRow.size() - 7).trim(); // Get the intersection description
                IntersectionInf intersectionInf = parseIntersectionInformation(IntersectionInformation);
                int IntersectionID = intersectionInf.IntersectionID;
                String MainStreet = intersectionInf.MainStreet;
                String CrossStreet = intersectionInf.CrossStreet;

                int DetectorID = Integer.parseInt(dataRow.get(dataRow.size() - 8).trim());
                String DescriptionInf = dataRow.get(0).trim(); // Get the description of data samples
                DateTimeAttributes dateTimeAttributes=parseDateTimeAttributes(DescriptionInf);
                int Date = dateTimeAttributes.Date;
                int Time = dateTimeAttributes.Time;

                String DetectorInf;
                if(dataRow.size()>11){
                    DetectorInf=dataRow.get(2).trim()+"+"+dataRow.get(3).trim();
                }else{
                    DetectorInf=dataRow.get(2).trim();
                }
                DetectorNameAttributes detectorNameAttributes=parseDetectorName(DetectorInf);
                String Lane = detectorNameAttributes.Lane;
                String Direction = detectorNameAttributes.Direction;
                String Type = detectorNameAttributes.Type;
                String Description = detectorNameAttributes.Description;

                DetectorData detectorData = new DetectorData(DetectorID, IntersectionID, Date, Time, Period, Volume,
                        Occupancy, Speed, Invalid, FaultCode);
                detectorDataList.add(detectorData);

                DetectorInventory detectorInventory = new DetectorInventory(IntersectionID, DetectorID, Direction,Date,
                        Type, Lane, MainStreet, CrossStreet, Description);
                detectorInventoryList.add(detectorInventory);
            }
            br.close();
            fr.close();
            PasadenaData pasadenaData=new PasadenaData(detectorInventoryList,detectorDataList);
            return pasadenaData;
        }catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    public static DateTimeAttributes parseDateTimeAttributes(String DescriptionInf){
        // This function is used to parse the Date and Time string

        String [] tmpString=DescriptionInf.split(" ");
        //Get Date and Time
        String DateStr=tmpString[0].trim();
        String TimeStr=tmpString[1].trim();
        tmpString=DateStr.split("-");
        int Date=Integer.parseInt(tmpString[0].trim())*10000+Integer.parseInt(tmpString[1].trim())*100+
                Integer.parseInt(tmpString[2].trim());
        tmpString=TimeStr.split(":");
        int Time=Integer.parseInt(tmpString[0].trim())*3600+Integer.parseInt(tmpString[1].trim())*60+
                (int)Double.parseDouble(tmpString[2].trim());

        DateTimeAttributes dateTimeAttributes=new DateTimeAttributes(Date, Time);
        return dateTimeAttributes;
    }

    public static DetectorNameAttributes parseDetectorName(String DescriptionInf){
        // This function is used to parse the detector name

        String [] tmpString= DescriptionInf.split("#"); // Split by #
        String tmpDescription=tmpString[tmpString.length-1].trim(); // Get the last string
        String Description=tmpDescription.substring(3,tmpDescription.length()); // Get rid of the Intersection ID: assume 3 digits
        while(Description.substring(0,1).equals(" ")){// Get rid of the space
            Description=Description.substring(1,Description.length());
        }

        String Type=Description.substring(0,3); // Get the type: assume three digits

        String DirectionLane=Description.substring(3,Description.length());
        while(DirectionLane.substring(0,1).equals(" ")){// Get rid of space
            DirectionLane=DirectionLane.substring(1,DirectionLane.length());
        }
        String Direction=DirectionLane.substring(0,2); // Get the Direction: Assume two digits
        String Lane;
        if(Direction.equals("EX") || Direction.equals("LN")){
            //System.out.println(Direction+" @ "+ DirectionLane);
            Direction="NA";
            Lane=DirectionLane.substring(0,DirectionLane.length()); // Get the lane information
        }else{
            Lane=DirectionLane.substring(2,DirectionLane.length()); // Get the lane information
        }
        while(Lane.substring(0,1).equals(" ")){// Get rid of space
            Lane=Lane.substring(1,Lane.length());
        }
        DetectorNameAttributes detectorNameAttributes=new DetectorNameAttributes(Lane, Direction, Type, Description);
        return detectorNameAttributes;
    }

    public static IntersectionInf parseIntersectionInformation(String IntersectionInformation){
        // This function is used to return Intersection information

        IntersectionInf intersection;
        String [] tmpString;
        // Get Intersection ID
        tmpString=IntersectionInformation.split("#");
        String IntersectionIDStr=tmpString[1].substring(0,3); // Considering 3-digit signal ID
        int IntersectionID=Integer.parseInt(IntersectionIDStr);

        // Get MainStreet and CrossStreet
        String Streets=tmpString[1].substring(3,tmpString[1].length());
        tmpString=Streets.split("@");

        String MainStreet = tmpString[0];
        while(MainStreet.substring(0,1).equals(" ")) { // Get rid of space
            MainStreet = MainStreet.substring(1,MainStreet.length());
        }
        String CrossStreet=tmpString[1];
        while(CrossStreet.substring(0,1).equals(" ")) { // Get rid of space
            CrossStreet = CrossStreet.substring(1,CrossStreet.length());
        }
        intersection=new IntersectionInf(IntersectionID, MainStreet, CrossStreet);
        return intersection;
    }
}
