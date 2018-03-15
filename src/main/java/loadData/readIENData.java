package loadData;
import main.MainFunction;

// Import functions
import Utility.util;
import main.MainFunction;
import saveData.saveDataToDatabase;

import java.io.*;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;


public class readIENData {

    public static File[] listOfFiles;

    // ***********************************************************//
    // This is the main function to read the IEN data files
    public static void mainIENRead(){

        // Truncate tables for testing purposes
        //mainTruncateTables("Both","All",2017);

        // Get the list of files
        File fileDir = new File(MainFunction.cBlock.rawIENDataDir);
        listOfFiles = fileDir.listFiles();

        try{
            // Create the connections to the databases
            MainFunction.conArcadia = DriverManager.getConnection(MainFunction.hostIENArcadia,
                    MainFunction.userName, MainFunction.password);
            MainFunction.conLACO = DriverManager.getConnection(MainFunction.hostIENLACO,
                    MainFunction.userName, MainFunction.password);
            System.out.println("Succefully connect to the database!");

            //Create two workers for Arcadia and LACO
            MyRunnableArcadia worker1= new MyRunnableArcadia();
            Thread t1 = new Thread(worker1);
            t1.start();
            MyRunnableLACO worker2= new MyRunnableLACO();
            Thread t2 = new Thread(worker2);
            t2.start();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static class MyRunnableArcadia implements Runnable{
        // This is the class for runnable programs
        public void run(){
            readDataForArcadia();
        }
    }
    public static void readDataForArcadia(){
        // This function is used to read the IEN data for LACO

        long startTime, endTime;
        int curNumFile=0;
        int numFile=50;
        List<String> fileName= new ArrayList<String>();
        IENData ienData= inilizationIENData();
        uniqueDataWithHashSet curInputdata= initilizationUniqueDataWithHashSet();
        for (int i = 0; i < listOfFiles.length; i++) {// Loop for each file
            if (listOfFiles[i].isFile()) {
                // If it is a file
                if (listOfFiles[i].getName().contains("ienData_Arcadia")) {
                    //System.out.println("File: " + listOfFiles[i].getName());
                    String IENDataFileName = MainFunction.cBlock.rawIENDataDir + "\\" + listOfFiles[i].getName();

                    // Read the IEN data file
                    IENData tmpIenData = readIENDataFromFile(IENDataFileName);
                    curNumFile=curNumFile+1;
                    ienData=combineIENData(ienData,tmpIenData);
                    fileName.add(listOfFiles[i].getName());
                }
                // Save the data to the corresponding databases
                if(curNumFile==numFile || i==listOfFiles.length-1) {

                    //Remove duplicated values
                    startTime = System.currentTimeMillis();
                    curInputdata.ienData=ienData; // Update the IEN data
                    curInputdata= util.removeDuplicatedData_rev(curInputdata);// Get tje unique values with updated hash sets
                    IENData ienData1=curInputdata.ienData;
                    endTime = System.currentTimeMillis();
                    System.out.println("Time Used(Removing duplicates):" + (endTime - startTime));

                    startTime = System.currentTimeMillis();
                    saveDataToDatabase.insertIENDataToDataBaseBatch(MainFunction.conArcadia, MainFunction.conLACO, ienData1);
                    //saveDataToDatabase.insertIENDataToDataBase(conArcadia, conLACO, ienData1);
                    endTime = System.currentTimeMillis();
                    System.out.println("Time Used(Inserting):" + (endTime - startTime));

                    // Remove the files
                    for (int j=0;j<fileName.size();j++) {
                        util.moveFileFromAToB(MainFunction.cBlock.rawIENDataDir, MainFunction.cBlock.rawIENDataNewDir,
                                fileName.get(j));
                    }

                    ienData= inilizationIENData();
                    fileName= new ArrayList<String>();
                    curNumFile=0;
                }
            }
        }
    }

    public static class MyRunnableLACO implements Runnable{
        // This is the class for runnable programs
        public void run(){
            readDataForLACO();
        }
    }
    public static void readDataForLACO(){
        // This function is used to read the IEN data for LACO

        long startTime, endTime;
        int curNumFile=0;
        int numFile=50;
        List<String> fileName= new ArrayList<String>();
        IENData ienData= inilizationIENData();
        uniqueDataWithHashSet curInputdata=initilizationUniqueDataWithHashSet();
        for (int i = 0; i < listOfFiles.length; i++) {// Loop for each file
            if (listOfFiles[i].isFile()) {
                // If it is a file
                if (listOfFiles[i].getName().contains("ienData_LACO")) {
                    //System.out.println("File: " + listOfFiles[i].getName());
                    String IENDataFileName = MainFunction.cBlock.rawIENDataDir + "\\" + listOfFiles[i].getName();

                    // Read the IEN data file
                    IENData tmpIenData = readIENDataFromFile(IENDataFileName);
                    curNumFile=curNumFile+1;
                    ienData=combineIENData(ienData,tmpIenData);
                    fileName.add(listOfFiles[i].getName());
                }
                // Save the data to the corresponding databases
                if(curNumFile==numFile || i==listOfFiles.length-1) {

                    //Remove duplicated values
                    startTime = System.currentTimeMillis();
                    curInputdata.ienData=ienData; // Update the IEN data
                    curInputdata= util.removeDuplicatedData_rev(curInputdata);// Get the unique values with updated hash sets
                    IENData ienData1=curInputdata.ienData;
                    endTime = System.currentTimeMillis();
                    System.out.println("Time Used(Removing duplicates):" + (endTime - startTime));

                    startTime = System.currentTimeMillis();
                    saveDataToDatabase.insertIENDataToDataBaseBatch(MainFunction.conArcadia, MainFunction.conLACO, ienData1);
                    //saveDataToDatabase.insertIENDataToDataBase(conArcadia, conLACO, ienData1);
                    endTime = System.currentTimeMillis();
                    System.out.println("Time Used(Inserting):" + (endTime - startTime));

                    // Remove the files
                    for (int j=0;j<fileName.size();j++) {
                        util.moveFileFromAToB(MainFunction.cBlock.rawIENDataDir, MainFunction.cBlock.rawIENDataNewDir,
                                fileName.get(j));
                    }

                    ienData= inilizationIENData();
                    fileName= new ArrayList<String>();
                    curNumFile=0;
                }
            }
        }
    }

    //***********Below are the major functions **************
    public static class IENData{
        // This is the class of the IEN Data
        public IENData(List<DevInv> _listDevInv, List<DevData> _listDevData, List<IntSigInv> _listIntSigInv,
                              List<IntSigData> _listIntSigData, List<PlanPhase> _listPlanPhase, List<LastCyclePhase> _listLastCyclePhase){
            this.listDevInv=_listDevInv;
            this.listDevData=_listDevData;
            this.listIntSigInv=_listIntSigInv;
            this.listIntSigData=_listIntSigData;
            this.listPlanPhase=_listPlanPhase;
            this.listLastCyclePhase=_listLastCyclePhase;
        }

        public List<DevInv> listDevInv;                 // Detector inventory
        public List<DevData> listDevData;               // Detector data
        public List<IntSigInv> listIntSigInv;           // Intersection signal inventory
        public List<IntSigData> listIntSigData;         // Intersection signal data
        public List<PlanPhase> listPlanPhase;           // Planned phase settings
        public List<LastCyclePhase> listLastCyclePhase; // Last-cycle phase settings;
    }

    public static class DevInv{
        // This is the class of the IEN detector inventory

        public DevInv(String _orgID,int _devID,int _date,int _time,String _description,String _roadwayName,String _crossStreet,double _latitude,
                      double _longitude,String _direction,int _avgPeriod,int _associatedIntID){
            this.orgID=_orgID;
            this.devID=_devID;
            this.date=_date;
            this.time=_time;
            this.description=_description;
            this.roadwayName=_roadwayName;
            this.crossStreet=_crossStreet;
            this.latitude=_latitude;
            this.longitude=_longitude;
            this.direction=_direction;
            this.avgPeriod=_avgPeriod;
            this.associatedIntID=_associatedIntID;
        }
        public String orgID;
        public int devID;
        public int date;
        public int time;
        public String description;
        public String roadwayName;
        public String crossStreet;
        public double latitude;
        public double longitude;
        public String direction;
        public int avgPeriod;
        public int associatedIntID;
    }

    public static class DevData{
        // This is the class of the IEN detector data
        public DevData(String _orgID, int _devID, int _date, int _time, String _state, double _speed, double _occupancy,
                       double _volume, double _avgSpeed, double _avgOccupancy, double _avgVolume){
            this.orgID=_orgID;
            this.devID=_devID;
            this.date=_date;
            this.time=_time;
            this.state=_state;
            this.speed=_speed;
            this.occupancy=_occupancy;
            this.volume=_volume;
            this.avgSpeed=_avgSpeed;
            this.avgOccupancy=_avgOccupancy;
            this.avgVolume=_avgVolume;
        }
        public String orgID;               // Organization ID
        public int devID;                  // Detector ID
        public int date;                   // Date
        public int time;                   // Time
        public String state;               // State
        public double speed;               // Speed
        public double occupancy;           // Occupancy
        public double volume;              // Volume
        public double avgSpeed;            // Average Speed
        public double avgOccupancy;        // Average Occupancy
        public double avgVolume;           // Average Volume
    }

    public static class PlanPhase{
        // This is the class of the IEN planned phase data
        public PlanPhase(String _orgID, int _intID, int _date, int _time, String _phaseIDTime){
            this.orgID=_orgID;
            this.intID=_intID;
            this.date=_date;
            this.time=_time;
            this.phaseIDTime=_phaseIDTime;
        }
        public String orgID;           // Organization ID
        public int intID;              // Intersection ID
        public int date;               // Date
        public int time;               // Time
        public String phaseIDTime;     // Phase ID and Time
    }

    public static class LastCyclePhase{
        // This is the class of the IEN last-cycle phase data
        public LastCyclePhase(String _orgID, int _intID, int _date, int _time, int _cycleLength, String _phaseIDTime){
            this.orgID=_orgID;
            this.intID=_intID;
            this.date=_date;
            this.time=_time;
            this.cycleLength=_cycleLength;
            this.phaseIDTime=_phaseIDTime;
        }
        public String orgID;           // Organization ID
        public int intID;              // Intersection ID
        public int date;               // Date
        public int time;               // Time
        public int cycleLength;        // Cycle length
        public String phaseIDTime;     // Phase ID and Time
    }

    public static class IntSigData{
        // This is the class of the IEN intersection signal data
        public IntSigData(String _orgID,int _intID,int _date,int _time,String _commState,String _sigState,int _timingPlan,
                int _desiredCycleLength,int _desiredOffset,int _actualOffset,String _controlMode){
            this.orgID=_orgID;
            this.intID=_intID;
            this.date=_date;
            this.time=_time;
            this.commState=_commState;
            this.sigState=_sigState;
            this.timingPlan=_timingPlan;
            this.desiredCycleLength=_desiredCycleLength;
            this.desiredOffset=_desiredOffset;
            this.actualOffset=_actualOffset;
            this.controlMode=_controlMode;
        }
        public String orgID;
        public int intID;
        public int date;
        public int time;
        public String commState;
        public String sigState;
        public int timingPlan;
        public int desiredCycleLength;
        public int desiredOffset;
        public int actualOffset;
        public String controlMode;
    }

    public static class IntSigInv{
        // This is the class of the IEN intersection inventory data
        public IntSigInv(String _orgID,int _intID,int _date,int _time,String _signalType,String _description,String _mainStreet,String _crossStreet,
                      double _latitude,double _longitude){
            this.orgID=_orgID;
            this.intID=_intID;
            this.date=_date;
            this.time=_time;
            this.signalType=_signalType;
            this.description=_description;
            this.mainStreet=_mainStreet;
            this.crossStreet=_crossStreet;
            this.latitude=_latitude;
            this.longitude=_longitude;
        }
        public String orgID;
        public int intID;
        public int date;
        public int time;
        public String signalType;
        public String description;
        public String mainStreet;
        public String crossStreet;
        public double latitude;
        public double longitude;
    }

    public static IENData combineIENData(IENData ienData_A, IENData ienData_B){
        // This function is used to combine IEN data files

        // Detector data
        ienData_A.listDevData.addAll(ienData_B.listDevData);
        // Detector inventory
        ienData_A.listDevInv.addAll(ienData_B.listDevInv);
        // Intersection signal inventory
        ienData_A.listIntSigInv.addAll(ienData_B.listIntSigInv);
        /* Intersection signal data */
        ienData_A.listIntSigData.addAll(ienData_B.listIntSigData);
        // plan Phase
        ienData_A.listPlanPhase.addAll(ienData_B.listPlanPhase);
        // Last Cycle Phase
        ienData_A.listLastCyclePhase.addAll(ienData_B.listLastCyclePhase);

        return ienData_A;
    }

    public static class uniqueDataWithHashSet{
        public uniqueDataWithHashSet(IENData _ienData,HashSet<String> _setDevData, HashSet<String> _setDevInv,
                                     HashSet<String> _setIntSigData, HashSet<String> _setIntSigInv, HashSet<String> _setPlanPhase,
                                     HashSet<String> _setLastCyclePhase){
            this.ienData=_ienData;
            this.setDevData=_setDevData;
            this.setDevInv=_setDevInv;
            this.setIntSigData=_setIntSigData;
            this.setIntSigInv=_setIntSigInv;
            this.setPlanPhase=_setPlanPhase;
            this.setLastCyclePhase=_setLastCyclePhase;
        }
        public IENData ienData;
        public HashSet<String> setDevData;
        public HashSet<String> setDevInv;
        public HashSet<String> setIntSigInv;
        public HashSet<String> setIntSigData;
        public HashSet<String> setPlanPhase;
        public HashSet<String> setLastCyclePhase;
    }

    public static uniqueDataWithHashSet initilizationUniqueDataWithHashSet(){

        IENData ienData= inilizationIENData();
        HashSet<String> setDevData= new HashSet<String>();
        HashSet<String> setDevInv= new HashSet<String>();
        HashSet<String> setIntSigInv= new HashSet<String>();
        HashSet<String> setIntSigData= new HashSet<String>();
        HashSet<String> setPlanPhase= new HashSet<String>();
        HashSet<String> setLastCyclePhase= new HashSet<String>();

        uniqueDataWithHashSet uniqueData=new uniqueDataWithHashSet(ienData,setDevData, setDevInv,setIntSigData,
                setIntSigInv, setPlanPhase,setLastCyclePhase);
        return uniqueData;
    }

    public static IENData inilizationIENData(){

        List<DevInv> listDevInv = new ArrayList<DevInv>();                              // Detector inventory
        List<DevData> listDevData = new ArrayList<DevData>();                           // Detector data
        List<IntSigInv> listIntSigInv = new ArrayList<IntSigInv>();                     // Intersection signal inventory
        List<IntSigData> listIntSigData = new ArrayList<IntSigData>();                  // Intersection signal data
        List<PlanPhase> listPlanPhase = new ArrayList<PlanPhase>();                     // Planned phase
        List<LastCyclePhase> listLastCyclePhase = new ArrayList<LastCyclePhase>();      // Last-Cycle phase

        IENData ienData= new IENData(listDevInv,listDevData,listIntSigInv,listIntSigData,listPlanPhase,listLastCyclePhase);
        return ienData;
    }

    public static IENData readIENDataFromFile(String IENDataFileName){
        // This function is used to read the IEN data

        List<DevInv> listDevInv = new ArrayList<DevInv>();                              // Detector inventory
        List<DevData> listDevData = new ArrayList<DevData>();                           // Detector data
        List<IntSigInv> listIntSigInv = new ArrayList<IntSigInv>();                     // Intersection signal inventory
        List<IntSigData> listIntSigData = new ArrayList<IntSigData>();                  // Intersection signal data
        List<PlanPhase> listPlanPhase = new ArrayList<PlanPhase>();                     // Planned phase
        List<LastCyclePhase> listLastCyclePhase = new ArrayList<LastCyclePhase>();      // Last-Cycle phase

        String tmpDevInvString;
        String tmpDevDataString;
        String tmpIntSigInvString;
        String tmpIntSigDataString;
        String tmpPlanPhaseString;
        String tmpLastCyclePhaseString;

        // Open a new file
        File ienFile = new File(IENDataFileName);

        int year = Calendar.getInstance().get(Calendar.YEAR);

        // Check the existence of the file
        if(!ienFile.exists())
        {
            System.out.println("Can not find the file!");
            return null;
        }

        // If the file exists, do the following steps
        try {
            FileReader frIEN = new FileReader(ienFile);
            BufferedReader brIEN = new BufferedReader(frIEN);

            String text = null;
            String [] tmpArray;
            String [] tmpDateTime;
            String [] tmpPhase;
            while ((text = brIEN.readLine())!=null) {

                tmpArray=text.split(","); // Split strings

                //***********First: If it is for device inventory**************
                if(tmpArray[0].equals("Device Inventory list")) {

                    //Ignore the first line
                    brIEN.readLine();
                    while((text = brIEN.readLine()) != null && text.length()!=0) {
                        //If it is not the end or spaced
                        //Get the date and time
                        tmpDevInvString=stringProcessing(text,11, "DetInv");
                        // There may be something wrong in the YEAR/Month/Day output format in the IEN server
                        DevInv devInv=stringToDevInv(tmpDevInvString);
                        if(devInv.date/10000==year) {
                            listDevInv.add(devInv);
                        }
                    }
                }

                //***********Second: If it is for device data**************
                if(tmpArray[0].equals("Device Data")) {

                    //Ignore the first line
                    brIEN.readLine();
                    while((text = brIEN.readLine()) != null && text.length()!=0) {
                        tmpDevDataString=stringProcessing(text,10,"DetData");
                        // There may be something wrong in the YEAR/Month/Day output format in the IEN server
                        DevData devData=stringToDevData(tmpDevDataString);
                        if(devData.date/10000==year){
                            listDevData.add(devData);
                        }
                    }
                }

                //***********Third: If it is for intersection signal inventory**************
                if(tmpArray[0].equals("Intersection Signal Inventory list")) {

                    //Ignore the first line
                    brIEN.readLine();
                    while((text = brIEN.readLine()) != null && text.length()!=0) {
                        tmpIntSigInvString=stringProcessing(text,9,"SigInv");
                        IntSigInv intSigInv=stringToIntSigInv(tmpIntSigInvString);
                        if(intSigInv.date/10000==year){
                            listIntSigInv.add(intSigInv);
                        }
                    }
                }

                //***********Fourth: If it is for intersection signal data*************
                if(tmpArray[0].equals("Intersection Signal Data")) {

                    //Ignore the first line
                    brIEN.readLine();
                    while((text = brIEN.readLine()) != null && text.length()!=0) {
                        tmpIntSigDataString=stringProcessing(text,10,"SigData");
                        IntSigData intSigData=stringToIntSigData(tmpIntSigDataString);
                        if(intSigData.date/10000==year) {
                            listIntSigData.add(intSigData);
                        }
                    }
                }

                //***********Fifth: If it is for intersection planned phases *************
                if(tmpArray[0].equals("Intersection Signal Planned Phases")) {

                    //Ignore the first line
                    brIEN.readLine();
                    while((text = brIEN.readLine()) != null && text.length()!=0) {
                        tmpArray=text.split(",");
                        if(tmpArray.length!=4){
                            System.out.println("Wrong input string type!");
                            return null;
                        }
                        tmpArray[0] = tmpArray[0].replace(" ", ""); //Get rid of space
                        tmpArray[1] = tmpArray[1].replace(" ", ""); //Get rid of space
                        tmpArray[3] = tmpArray[3].replace(" ", ""); //Get rid of space

                        tmpDateTime = (tmpArray[2]).split(" ");

                        tmpPhase =(tmpArray[3]).split("\\[");
                        tmpPhase =(tmpPhase[1]).split("]");

                        tmpPlanPhaseString=tmpArray[0]+","+tmpArray[1]+","+tmpDateTime[1]+","+tmpDateTime[2]+","+tmpPhase[0];
                        PlanPhase planPhase=stringToPlanPhase(tmpPlanPhaseString);
                        if(planPhase.date/10000==year){
                            listPlanPhase.add(planPhase);
                        }
                    }
                }

                //***********Sixth: If it is for intersection last-cycle phases *************
                if(tmpArray[0].equals("Intersection Signal Last Cycle Phases")) {

                    //Ignore the first line
                    brIEN.readLine();
                    while((text = brIEN.readLine()) != null && text.length()!=0) {
                        tmpArray=text.split(",");
                        if(tmpArray.length!=5){
                            System.out.println("Wrong input string type!");
                            return null;
                        }
                        tmpArray[0] = tmpArray[0].replace(" ", ""); //Get rid of space
                        tmpArray[1] = tmpArray[1].replace(" ", ""); //Get rid of space
                        tmpArray[3] = tmpArray[3].replace(" ", ""); //Get rid of space
                        tmpArray[4] = tmpArray[4].replace(" ", ""); //Get rid of space

                        tmpDateTime = (tmpArray[2]).split(" ");
                        tmpPhase =(tmpArray[4]).split("\\[");
                        tmpPhase =(tmpPhase[1]).split("]");

                        tmpLastCyclePhaseString=tmpArray[0]+","+tmpArray[1]+","+tmpDateTime[1]
                                +","+tmpDateTime[2]+","+tmpArray[3]+","+tmpPhase[0];
                        LastCyclePhase lastCyclePhase=stringToLastCyclePhase(tmpLastCyclePhaseString);
                        if(lastCyclePhase.date/10000==year){
                            listLastCyclePhase.add(lastCyclePhase);
                        }
                    }
                }
            }
            brIEN.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        IENData ienData= new IENData(listDevInv,listDevData,listIntSigInv,listIntSigData,listPlanPhase,listLastCyclePhase);
        return ienData;
    }

    public static LastCyclePhase stringToLastCyclePhase(String inputString){
        // This function is to convert string to last-cycle setting

        String[] tmpString=inputString.split(",");
        String orgID=tmpString[0];
        int intID=Integer.parseInt(tmpString[1]);

        tmpString[2]=tmpString[2].replace(".",":");
        String [] tmpDate=tmpString[2].split(":");
        int date=Integer.parseInt(tmpDate[0])*10000+Integer.parseInt(tmpDate[1])*100+Integer.parseInt(tmpDate[2]);

        String [] tmpTime=tmpString[3].split(":");
        int time=Integer.parseInt(tmpTime[0])*3600+Integer.parseInt(tmpTime[1])*60+Integer.parseInt(tmpTime[2]);

        int cycleLength=(int) Math.max(Double.parseDouble(tmpString[4]),0);
        String phaseIDTime=tmpString[5]; // Phase ID and Time

        LastCyclePhase lastCyclePhase= new LastCyclePhase(orgID,intID,date,time,cycleLength,phaseIDTime);
        return lastCyclePhase;
    }

    public static PlanPhase stringToPlanPhase(String inputString){
        // This function is to convert string to PlanPhase

        String[] tmpString=inputString.split(",");
        String orgID=tmpString[0];
        int intID=Integer.parseInt(tmpString[1]);

        tmpString[2]=tmpString[2].replace(".",":");
        String [] tmpDate=tmpString[2].split(":");
        int date=Integer.parseInt(tmpDate[0])*10000+Integer.parseInt(tmpDate[1])*100+Integer.parseInt(tmpDate[2]);

        String [] tmpTime=tmpString[3].split(":");
        int time=Integer.parseInt(tmpTime[0])*3600+Integer.parseInt(tmpTime[1])*60+Integer.parseInt(tmpTime[2]);

        String phaseIDTime=tmpString[4]; // Phase ID and Time

        PlanPhase planPhase= new PlanPhase(orgID,intID,date,time,phaseIDTime);
        return planPhase;
    }

    public static IntSigData stringToIntSigData(String inputString){
        // This function is to convert string to IntSigData

        String[] tmpString=inputString.split(",");
        String orgID=tmpString[0];
        int intID=Integer.parseInt(tmpString[1]);

        tmpString[2]=tmpString[2].replace(".",":");
        String [] tmpDate=tmpString[2].split(":");
        int date=Integer.parseInt(tmpDate[0])*10000+Integer.parseInt(tmpDate[1])*100+Integer.parseInt(tmpDate[2]);

        String [] tmpTime=tmpString[3].split(":");
        int time=Integer.parseInt(tmpTime[0])*3600+Integer.parseInt(tmpTime[1])*60+Integer.parseInt(tmpTime[2]);

        String commState=tmpString[4];
        String sigState=tmpString[5];
        int timingPlan=(int) Math.max(Double.parseDouble(tmpString[6]),0);
        int desiredCycleLength=(int) Math.max(Double.parseDouble(tmpString[7]),0);
        int desiredOffset=(int) Math.max(Double.parseDouble(tmpString[8]),0);
        int actualOffset=(int) Math.max(Double.parseDouble(tmpString[9]),0);
        String controlMode=tmpString[10];

        IntSigData intSigData= new IntSigData(orgID,intID,date,time,commState,sigState,timingPlan,desiredCycleLength,
                desiredOffset,actualOffset,controlMode);
        return intSigData;
    }

    public static IntSigInv stringToIntSigInv(String inputString){
        // This function is to convert string to IntSigInv

        String[] tmpString=inputString.split(",");
        String orgID=tmpString[0];
        int intID=Integer.parseInt(tmpString[1]);

        tmpString[2]=tmpString[2].replace(".",":");
        String [] tmpDate=tmpString[2].split(":");
        int date=Integer.parseInt(tmpDate[0])*10000+Integer.parseInt(tmpDate[1])*100+Integer.parseInt(tmpDate[2]);

        String [] tmpTime=tmpString[3].split(":");
        int time=Integer.parseInt(tmpTime[0])*3600+Integer.parseInt(tmpTime[1])*60+Integer.parseInt(tmpTime[2]);

        String signalType=tmpString[4];
        String description=tmpString[5];
        String mainStreet=tmpString[6];
        String crossStreet=tmpString[7];
        double latitude=Double.parseDouble(tmpString[8])/1000000.0;
        double longitude=Double.parseDouble(tmpString[9])/1000000.0;

        IntSigInv intSigInv= new IntSigInv(orgID,intID,date,time,signalType,description,mainStreet,crossStreet,latitude,longitude);
        return intSigInv;
    }

    public static DevData stringToDevData(String inputString){
        // This function is to convert string to DevData

        String[] tmpString=inputString.split(",");
        String orgID=tmpString[0];
        int devID=Integer.parseInt(tmpString[1]);

        tmpString[2]=tmpString[2].replace(".",":");
        String [] tmpDate=tmpString[2].split(":");
        int date=Integer.parseInt(tmpDate[0])*10000+Integer.parseInt(tmpDate[1])*100+Integer.parseInt(tmpDate[2]);

        String [] tmpTime=tmpString[3].split(":");
        int time=Integer.parseInt(tmpTime[0])*3600+Integer.parseInt(tmpTime[1])*60+Integer.parseInt(tmpTime[2]);

        String state=tmpString[4];
        double speed=Math.max(Double.parseDouble(tmpString[5]),0);
        double occupancy=Math.max(Double.parseDouble(tmpString[6]),0);
        double volume=Math.max(Double.parseDouble(tmpString[7]),0);
        double avgSpeed=Math.max(Double.parseDouble(tmpString[8]),0);
        double avgOccupancy=Math.max(Double.parseDouble(tmpString[9]),0);
        double avgVolume=Math.max(Double.parseDouble(tmpString[10]),0);

        DevData devData=new DevData(orgID,devID,date,time,state,speed,occupancy,volume,
                avgSpeed,avgOccupancy,avgVolume);

        return devData;
    }

    public static DevInv stringToDevInv(String inputString){
        // This function is to convert string to DevInv

        String[] tmpString=inputString.split(",");
        String orgID=tmpString[0];
        int devID=Integer.parseInt(tmpString[1]);

        tmpString[2]=tmpString[2].replace(".",":");
        String [] tmpDate=tmpString[2].split(":");
        int date=Integer.parseInt(tmpDate[0])*10000+Integer.parseInt(tmpDate[1])*100+Integer.parseInt(tmpDate[2]);

        String [] tmpTime=tmpString[3].split(":");
        int time=Integer.parseInt(tmpTime[0])*3600+Integer.parseInt(tmpTime[1])*60+Integer.parseInt(tmpTime[2]);

        String description=tmpString[4];
        String roadwayName=tmpString[5];
        String crossStreet=tmpString[6];
        double latitude=Double.parseDouble(tmpString[7])/1000000.0;
        double longitude=Double.parseDouble(tmpString[8])/1000000.0;
        String direction=tmpString[9];
        int avgPeriod=(int) Double.parseDouble(tmpString[10]);
        int associatedIntID=(int)Double.parseDouble(tmpString[11]);

        DevInv devInv=new DevInv(orgID,devID,date,time,description,roadwayName,crossStreet,
                latitude,longitude,direction,avgPeriod,associatedIntID);

        return devInv;
    }

    public static String stringProcessing(String text,int DefaultLength, String Type) {
        // This is the function to process the string

        String [] tmpArray;
        String [] tmpDateTime;
        String tmpString;

        tmpArray=text.split(","); // Split by ","
        // For OrgID and DevID/IntID
        tmpArray[0] = tmpArray[0].replace(" ", ""); //Get rid of space
        tmpArray[1] = tmpArray[1].replace(" ", ""); //Get rid of space

        // For date and time
        tmpDateTime = (tmpArray[2]).split(" "); // Split date and time
        tmpString=tmpArray[0]+","+tmpArray[1]+","+tmpDateTime[1]+","+tmpDateTime[2]; // OrgID, DevID, date, time

        int addLoc;
        String tmp;
        if(Type.equals("DetInv")){ //If it is device inventory
            // Reconstruct the description part
            if(tmpArray.length==DefaultLength){
                tmp=tmpArray[3];
                addLoc=4;
            }
            else if(tmpArray.length==DefaultLength+1){
                tmp=tmpArray[3]+"&"+tmpArray[4];
                addLoc=5;
            }else if(tmpArray.length==DefaultLength+2){
                tmp=tmpArray[3]+"&"+tmpArray[4]+"&"+tmpArray[5];
                addLoc=6;
            }else{
                System.out.println("Wrong input string type!");
                return null;
            }
        }
        else if(Type.equals("SigInv")){ //If it is signal inventory
            // Reconstruct the description part
            if(tmpArray.length==DefaultLength){
                tmp=tmpArray[4];
                addLoc=5;
            }
            else if(tmpArray.length==DefaultLength+1){
                tmp=tmpArray[4]+"&"+tmpArray[5];
                addLoc=6;
            }else if(tmpArray.length==DefaultLength+2){
                tmp=tmpArray[4]+"&"+tmpArray[5]+"&"+tmpArray[6];
                addLoc=7;
            }else{
                System.out.println("Wrong input string type!");
                return null;
            }
            tmp=tmpArray[3]+","+tmp;
        }
        else{ //For other cases
            // Reconstruct the description part
            if(tmpArray.length==DefaultLength){
                tmp=tmpArray[3];
                addLoc=4;
            }
            else if(tmpArray.length==DefaultLength+1){
                tmp=tmpArray[3]+"&"+tmpArray[4];
                addLoc=5;
            }else if(tmpArray.length==DefaultLength+2){
                tmp=tmpArray[3]+"&"+tmpArray[4]+"&"+tmpArray[5];
                addLoc=6;
            }else{
                System.out.println("Wrong input string type!");
                return null;
            }
        }
        tmpString= tmpString+","+tmp;

        // Add the rest of the string
        for (int i=addLoc;i<tmpArray.length;i++)
        {
            if(tmpArray[i].equals(" ")) {
                tmpString = tmpString + "," + "NA";
            }
            else{
                tmpString= tmpString+","+tmpArray[i];
            }
        }
        return tmpString;
    }

}
