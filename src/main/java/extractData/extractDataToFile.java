package extractData;

import config.detectorConfig;
import Utility.util;
import main.MainFunction;
import sun.applet.Main;

import javax.swing.plaf.nimbus.State;
import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.lang.reflect.Field;
import java.util.Calendar;
import extractData.getDataFromDatabase.ClusterDetectorData;
import config.detectorConfig.DetectorProperty;

/**
 * Created by Qijian_Gan on 9/12/2017.
 */
public class extractDataToFile{

    public static SimpleDateFormat Year= new SimpleDateFormat("yyyy");
    public static SimpleDateFormat Month= new SimpleDateFormat("MM");
    public static SimpleDateFormat Day= new SimpleDateFormat("dd");

    // ***********************************************************//
    // This is the main function to extract data to file
    public static void mainExtractDataToFile(String host,String healthFileName , String curOrganization){
        String typeDaily="Daily";
        String typeWeekly="Weekly";
        String typeWeeklyNumber="WeeklyNumber";

        try {
            Connection con = DriverManager.getConnection(host, MainFunction.cBlock.userName, MainFunction.cBlock.password);
            System.out.println("Succefully connect to the database!");

            Date startDate=new SimpleDateFormat("yyy-MM-dd").parse(MainFunction.cBlock.startDateString);
            Date endDate=new SimpleDateFormat("yyy-MM-dd").parse(MainFunction.cBlock.endDateString);

            // Get the list of detector configuration
            List detConfigList;
            detConfigList=detectorConfig.readDetectorConfigFromDataBase(con,curOrganization,startDate);

            // Daily report
            File fileDir = new File(MainFunction.cBlock.healthOutputFolder);
            File fileNameDaily= new File(fileDir,healthFileName+"_"+typeDaily+".csv");
            // Weekly report
            File fileNameWeekly= new File(fileDir, healthFileName+"_"+typeWeekly+".csv");
            File fileNameWeeklyNum= new File(fileDir, healthFileName+"_"+typeWeeklyNumber+".csv");
            if(curOrganization.equals("Arcadia")) {
                int[][] healthIndex = outputHealthAnalysisToFileByDay(detConfigList, con, fileNameDaily, startDate, endDate);
                outputHealthAnalysisToFileByWeek(detConfigList,
                        healthIndex, fileNameWeekly, fileNameWeeklyNum, startDate, endDate);
            }
            if(curOrganization.equals("LACO")){
                int[][] healthIndex = outputHealthAnalysisToFileByDayLACO(detConfigList, con, fileNameDaily, startDate, endDate);
                outputHealthAnalysisToFileByWeekLACO(detConfigList,
                        healthIndex, fileNameWeekly, fileNameWeeklyNum, startDate, endDate);
            }
            if(curOrganization.equals("Pasadena")) {
                int[][] healthIndex = outputHealthAnalysisToFileByDay(detConfigList, con, fileNameDaily, startDate, endDate);
                outputHealthAnalysisToFileByWeekPasadena(detConfigList,
                        healthIndex, fileNameWeekly, fileNameWeeklyNum, startDate, endDate);
            }
            System.out.println("Done!");
        }catch (SQLException e)
        {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public  static int [][] outputHealthAnalysisToFileByDay(List detConfigList,Connection con,
                                               File fileName,Date startDate,Date endDate) {
        // This function is to output health analysis (daily) to files

        int numDay= (int) Math.round((endDate.getTime()-startDate.getTime())/1000.0/60.0/60.0/24.0)+1;
        int [][] healthIndex=new int[detConfigList.size()][numDay];

        try {
            FileWriter fw = new FileWriter(fileName);
            BufferedWriter bw = new BufferedWriter(fw);

            // Get the header
            String header="IntersectionName,IntersectionID,County,City,RoadName,Direction,"+
                    "SensorID,Movement,Status,DetourRoute";

            //Loop for all detectors
            for (int i=0;i<detConfigList.size();i++) {
                //Get the detector ID
                detectorConfig.DetectorProperty tmpProperty = (detectorConfig.DetectorProperty) detConfigList.get(i);
                int intID = tmpProperty.IntID;
                int sensorID = tmpProperty.SensorID;
                int detectorID = intID * 100 + sensorID;
                System.out.println(detectorID);

                //Get the string for each detector
                String detectorHealth=tmpProperty.IntName+","+tmpProperty.IntID+","+tmpProperty.County+","+
                        tmpProperty.City+","+tmpProperty.RoadName+","+tmpProperty.Direction+","+tmpProperty.SensorID+
                        ","+tmpProperty.Movement+","+tmpProperty.Status+","+tmpProperty.DetourRoute;
                //Loop for all days
                Date tmpDay = startDate;
                for (int j=0;j<numDay;j++){

                    int year = new Integer(Year.format(tmpDay));
                    int month = new Integer(Month.format(tmpDay));
                    int day = new Integer(Day.format(tmpDay));
                    if(i==0){
                        //Add to the header
                        header=header+","+(year*10000+month*100+day);
                    }

                    //Create the SQL statement
                    Statement sqlStatement = con.createStatement();
                    String query = "select * from detector_health where DetectorID=" + detectorID
                            + " and Year=" + year + " and Month=" + month + " and Day=" + day;

                    //Check the results
                    ResultSet result = sqlStatement.executeQuery(query);
                    if (result.next()) {
                        if(result.getInt("Health")==1) { // If Good
                            //System.out.println(result.getInt("DetectorID") + " &Date=" + tmpDay + "& Health=Good");

                            healthIndex[i][j]=1;
                            detectorHealth=detectorHealth+",Good";
                        }else { //If Bad
                            //System.out.println(result.getInt("DetectorID") + " &Date=" + tmpDay + "& Health=Bad");

                            healthIndex[i][j]=0;
                            detectorHealth=detectorHealth+",Bad";
                        }
                    }
                    else{//No data
                        //System.out.println(detectorID + " &Date=" + tmpDay + "& Health=No Data");
                        healthIndex[i][j]=-1;
                        detectorHealth=detectorHealth+",NoData";
                    }

                    //Add one day
                    tmpDay = util.addDays(tmpDay, 1);
                }

                if(i==0){ // For the first line, write header
                    header=header+"\n";
                    bw.write(header);
                }
                // Write the health of each detector
                detectorHealth=detectorHealth+"\n";
                bw.write(detectorHealth);
            }
            bw.close();
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return healthIndex;
    }

    public  static int [][] outputHealthAnalysisToFileByDayLACO(List detConfigList,Connection con,
                                                            File fileName,Date startDate,Date endDate) {
        // This function is to output health analysis (daily) to files

        int numDay= (int) Math.round((endDate.getTime()-startDate.getTime())/1000.0/60.0/60.0/24.0)+1;
        int [][] healthIndex=new int[detConfigList.size()][numDay];

        try {
            FileWriter fw = new FileWriter(fileName);
            BufferedWriter bw = new BufferedWriter(fw);

            // Get the header
            String header="IntersectionName,IntersectionID,County,City,RoadName,Direction,"+
                    "SensorID,Movement";

            //Loop for all detectors
            for (int i=0;i<detConfigList.size();i++) {
                //Get the detector ID
                detectorConfig.DetectorProperty tmpProperty = (detectorConfig.DetectorProperty) detConfigList.get(i);
                int intID = tmpProperty.IntID;
                int sensorID = tmpProperty.SensorID;
                int detectorID = sensorID;
                System.out.println("Intersection ID="+intID+" and Sensor ID="+detectorID);

                //Get the string for each detector
                String detectorHealth=tmpProperty.IntName+","+tmpProperty.IntID+","+tmpProperty.County+","+
                        tmpProperty.City+","+tmpProperty.RoadName+","+tmpProperty.Direction+","+tmpProperty.SensorID+
                        ","+tmpProperty.Movement;
                //Loop for all days
                Date tmpDay = startDate;
                for (int j=0;j<numDay;j++){

                    int year = new Integer(Year.format(tmpDay));
                    int month = new Integer(Month.format(tmpDay));
                    int day = new Integer(Day.format(tmpDay));
                    if(i==0){
                        //Add to the header
                        header=header+","+(year*10000+month*100+day);
                    }

                    //Create the SQL statement
                    Statement sqlStatement = con.createStatement();
                    String query = "select * from detector_health where DetectorID=" + detectorID
                            + " and Year=" + year + " and Month=" + month + " and Day=" + day;

                    //Check the results
                    ResultSet result = sqlStatement.executeQuery(query);
                    if (result.next()) {
                        if(result.getInt("Health")==1) { // If Good
                            //System.out.println(result.getInt("DetectorID") + " &Date=" + tmpDay + "& Health=Good");

                            healthIndex[i][j]=1;
                            detectorHealth=detectorHealth+",Good";
                        }else { //If Bad
                            //System.out.println(result.getInt("DetectorID") + " &Date=" + tmpDay + "& Health=Bad");

                            healthIndex[i][j]=0;
                            detectorHealth=detectorHealth+",Bad";
                        }
                    }
                    else{//No data
                        //System.out.println(detectorID + " &Date=" + tmpDay + "& Health=No Data");
                        healthIndex[i][j]=-1;
                        detectorHealth=detectorHealth+",NoData";
                    }

                    //Add one day
                    tmpDay = util.addDays(tmpDay, 1);
                }

                if(i==0){ // For the first line, write header
                    header=header+"\n";
                    bw.write(header);
                }
                // Write the health of each detector
                detectorHealth=detectorHealth+"\n";
                bw.write(detectorHealth);
            }
            bw.close();
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return healthIndex;
    }

    public static void outputHealthAnalysisToFileByWeek(List detConfigList, int [][] healthIndex, File fileNameWeekly,
                                                        File fileNameWeeklyNum, Date startDate, Date endDate){
        // This function is to output health analysis (weekly) to files

        try{
            // Get the header
            FileWriter fw = new FileWriter(fileNameWeekly);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("Weekly Data Quality(%), , Arcadia\n");
            bw.write(" ,Detour Route, , ,Not Detour Route\n");
            bw.write(" ,Good,Bad ,No Data,Good,Bad,No Data\n");

            FileWriter fwNum = new FileWriter(fileNameWeeklyNum);
            BufferedWriter bwNum = new BufferedWriter(fwNum);
            bwNum.write("Weekly Data Quality(Number), , Arcadia\n");
            bwNum.write(" ,Detour Route, , ,Not Detour Route\n");
            bwNum.write(" ,Good,Bad ,No Data,Good,Bad,No Data\n");

            // Check the starting of Sunday
            Calendar c= Calendar.getInstance();
            Date tmpDay=startDate;
            c.setTime(tmpDay);
            int dayOfWeek= c.get(Calendar.DAY_OF_WEEK);
            int curAddress=0;
            while (dayOfWeek!=1) {// If not Sunday
                tmpDay=util.addDays(tmpDay,1);//Add one day, check again
                c.setTime(tmpDay);
                dayOfWeek= c.get(Calendar.DAY_OF_WEEK);

                curAddress=curAddress+1; //Shift one day on the matrix column number
            }

            //Get the number of days
            int numDay=healthIndex[0].length;

            for (int i=curAddress;i<numDay;i=i+7){ // Loop for all days

                //System.out.println(i);
                int numGoodDetour=0;
                int numBadDetour=0;
                int numNodataDetour=0;

                int numGoodNoDetour=0;
                int numBadNoDetour=0;
                int numNodataNoDetour=0;

                int year = new Integer(Year.format(tmpDay));
                int month = new Integer(Month.format(tmpDay));
                int day = new Integer(Day.format(tmpDay));
                int _year = new Integer(Year.format(util.addDays(tmpDay,6)));
                int _month = new Integer(Month.format(util.addDays(tmpDay,6)));
                int _day = new Integer(Day.format(util.addDays(tmpDay,6)));
                String strWeek=(year*10000+month*100+day)+" To "+(_year*10000+_month*100+_day)+",";
                String strWeekNum=strWeek;

                for (int k=i;k<i+7;k++){//Check each week
                    //System.out.println(k);
                    for (int j=0; j<healthIndex.length;j++){ //Loop for each detector
                        detectorConfig.DetectorProperty tmpProperty = (detectorConfig.DetectorProperty) detConfigList.get(j);
                        String detour=tmpProperty.DetourRoute;
                        if(detour.equals("YES")){
                            if(healthIndex[j][k]==-1)
                                numNodataDetour=numNodataDetour+1;
                            else if(healthIndex[j][k]==0)
                                numBadDetour=numBadDetour+1;
                            else
                                numGoodDetour=numGoodDetour+1;
                        }else{
                            if(healthIndex[j][k]==-1)
                                numNodataNoDetour=numNodataNoDetour+1;
                            else if(healthIndex[j][k]==0)
                                numBadNoDetour=numBadNoDetour+1;
                            else
                                numGoodNoDetour=numGoodNoDetour+1;
                        }
                    }
                }
                strWeekNum=strWeekNum+Math.round(numGoodDetour/7.0)+","+Math.round(numBadDetour/7.0)+","+Math.round(numNodataDetour/7.0)+","
                        +Math.round(numGoodNoDetour/7.0)+","+Math.round(numBadNoDetour/7.0)+","+Math.round(numNodataNoDetour/7.0)+"\n";

                // Get the total number of detector for detour and non-detour routes
                int totalDetour=numGoodDetour+numBadDetour+numNodataDetour;
                int totalNoDetour=numGoodNoDetour+numBadNoDetour+numNodataNoDetour;
                if(totalDetour==0) //Avoid zeros
                    totalDetour=1;
                if(totalNoDetour==0) //Avoid zeros
                    totalNoDetour=1;

                strWeek=strWeek+(numGoodDetour*100.0/totalDetour)+","+(numBadDetour*100.0/totalDetour)+","
                        +(numNodataDetour*100.0/totalDetour)+","+(numGoodNoDetour*100.0/totalNoDetour)+","
                        +(numBadNoDetour*100.0/totalNoDetour)+","+(numNodataNoDetour*100.0/totalNoDetour)+"\n";
                bw.write(strWeek);
                bwNum.write(strWeekNum);

                tmpDay=util.addDays(tmpDay,7);
            }
            bw.close();
            bwNum.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void outputHealthAnalysisToFileByWeekLACO(List detConfigList, int [][] healthIndex, File fileNameWeekly,
                                                        File fileNameWeeklyNum, Date startDate, Date endDate){
        // This function is to output health analysis (weekly) to files

        try{
            // Get the header
            FileWriter fw = new FileWriter(fileNameWeekly);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("Weekly Data Quality(%)\n");
            bw.write(", ,LACO, , ,Monrovia, , ,Duarte \n");
            bw.write(" ,Good,Bad,No Data,Good,Bad,No Data,Good,Bad,No Data\n");

            FileWriter fwNum = new FileWriter(fileNameWeeklyNum);
            BufferedWriter bwNum = new BufferedWriter(fwNum);
            bwNum.write("Weekly Data Quality(#)\n");
            bwNum.write(", ,LACO, , ,Monrovia, , ,Duarte \n");
            bwNum.write(" ,Good,Bad,No Data,Good,Bad,No Data,Good,Bad,No Data\n");

            // Check the starting of Sunday
            Calendar c= Calendar.getInstance();
            Date tmpDay=startDate;
            c.setTime(tmpDay);
            int dayOfWeek= c.get(Calendar.DAY_OF_WEEK);
            int curAddress=0;
            while (dayOfWeek!=1) {// If not Sunday
                tmpDay=util.addDays(tmpDay,1);//Add one day, check again
                c.setTime(tmpDay);
                dayOfWeek= c.get(Calendar.DAY_OF_WEEK);

                curAddress=curAddress+1; //Shift one day on the matrix column number
            }

            //Get the number of days
            int numDay=healthIndex[0].length;

            for (int i=curAddress;i<numDay;i=i+7){ // Loop for all days

                //System.out.println(i);
                int numGoodLACO=0;
                int numBadLACO=0;
                int numNodataLACO=0;

                int numGoodMonrovia=0;
                int numBadMonrovia=0;
                int numNodataMonrovia=0;

                int numGoodDuarte=0;
                int numBadDuarte=0;
                int numNodataDuarte=0;

                int year = new Integer(Year.format(tmpDay));
                int month = new Integer(Month.format(tmpDay));
                int day = new Integer(Day.format(tmpDay));
                int _year = new Integer(Year.format(util.addDays(tmpDay,6)));
                int _month = new Integer(Month.format(util.addDays(tmpDay,6)));
                int _day = new Integer(Day.format(util.addDays(tmpDay,6)));
                String strWeek=(year*10000+month*100+day)+" To "+(_year*10000+_month*100+_day)+",";
                String strWeekNum=strWeek;

                for (int k=i;k<i+7;k++){//Check each week
                    //System.out.println(k);
                    for (int j=0; j<healthIndex.length;j++){ //Loop for each detector
                        detectorConfig.DetectorProperty tmpProperty = (detectorConfig.DetectorProperty) detConfigList.get(j);
                        String detour=tmpProperty.City;
                        if(detour.equals("Monrovia")){
                            if(healthIndex[j][k]==1)
                                numGoodMonrovia=numGoodMonrovia+1;
                            else if(healthIndex[j][k]==0)
                                numBadMonrovia=numBadMonrovia+1;
                            else
                                numNodataMonrovia=numNodataMonrovia+1;
                        }else if(detour.equals("Duarte")){
                            if(healthIndex[j][k]==1)
                                numGoodDuarte=numGoodDuarte+1;
                            else if(healthIndex[j][k]==0)
                                numBadDuarte=numBadDuarte+1;
                            else
                                numNodataDuarte=numNodataDuarte+1;
                        }
                        else{
                            if(healthIndex[j][k]==1)
                                numGoodLACO=numGoodLACO+1;
                            else if(healthIndex[j][k]==0)
                                numBadLACO=numBadLACO+1;
                            else
                                numNodataLACO=numNodataLACO+1;
                        }
                    }
                }
                strWeekNum=strWeekNum+Math.round(numGoodLACO/7.0)+","+Math.round(numBadLACO/7.0)+","+Math.round(numNodataLACO/7.0)+","
                        +Math.round(numGoodMonrovia/7.0)+","+Math.round(numBadMonrovia/7.0)+","+Math.round(numNodataMonrovia/7.0)+","
                        +Math.round(numGoodDuarte/7.0)+","+Math.round(numBadDuarte/7.0)+","+Math.round(numNodataDuarte/7.0)+"\n";

                // Get the total numbers
                int totalLACO=numGoodLACO+numBadLACO+numNodataLACO;
                int totalMonrovia=numGoodMonrovia+numBadMonrovia+numNodataMonrovia;
                int totalDuarte=numGoodDuarte+numBadDuarte+numNodataDuarte;
                if(totalLACO==0) //Avoid zeros
                    totalLACO=1;
                if(totalMonrovia==0) //Avoid zeros
                    totalMonrovia=1;
                if(totalDuarte==0) //Avoid zeros
                    totalDuarte=1;

                strWeek=strWeek+(numGoodLACO*100.0/totalLACO)+","+(numBadLACO*100.0/totalLACO)+","+(numNodataLACO*100.0/totalLACO)
                        +","+(numGoodMonrovia*100.0/totalMonrovia)+","+(numBadMonrovia*100.0/totalMonrovia)+","+(numNodataMonrovia*100.0/totalMonrovia)
                        +","+(numGoodDuarte*100.0/totalDuarte)+","+(numBadDuarte*100.0/totalDuarte)+","+(numNodataDuarte*100.0/totalDuarte)+"\n";
                bw.write(strWeek);
                bwNum.write(strWeekNum);

                tmpDay=util.addDays(tmpDay,7);
            }
            bw.close();
            bwNum.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void outputHealthAnalysisToFileByWeekPasadena(List detConfigList, int [][] healthIndex, File fileNameWeekly,
                                                            File fileNameWeeklyNum, Date startDate, Date endDate){
        // This function is to output health analysis (weekly) to files

        try{
            // Get the header
            FileWriter fw = new FileWriter(fileNameWeekly);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("Weekly Data Quality(%)\n");
            bw.write(", ,Pasadena, \n");
            bw.write(" ,Good,Bad,No Data\n");

            FileWriter fwNum = new FileWriter(fileNameWeeklyNum);
            BufferedWriter bwNum = new BufferedWriter(fwNum);
            bwNum.write("Weekly Data Quality(#)\n");
            bwNum.write(", ,Pasadena, \n");
            bwNum.write(" ,Good,Bad,No Data\n");

            // Check the starting of Sunday
            Calendar c= Calendar.getInstance();
            Date tmpDay=startDate;
            c.setTime(tmpDay);
            int dayOfWeek= c.get(Calendar.DAY_OF_WEEK);
            int curAddress=0;
            while (dayOfWeek!=1) {// If not Sunday
                tmpDay=util.addDays(tmpDay,1);//Add one day, check again
                c.setTime(tmpDay);
                dayOfWeek= c.get(Calendar.DAY_OF_WEEK);

                curAddress=curAddress+1; //Shift one day on the matrix column number
            }

            //Get the number of days
            int numDay=healthIndex[0].length;

            for (int i=curAddress;i<numDay;i=i+7){ // Loop for all days

                //System.out.println(i);
                int numGoodPasadena=0;
                int numBadPasadena=0;
                int numNodataPasadena=0;

                int year = new Integer(Year.format(tmpDay));
                int month = new Integer(Month.format(tmpDay));
                int day = new Integer(Day.format(tmpDay));
                int _year = new Integer(Year.format(util.addDays(tmpDay,6)));
                int _month = new Integer(Month.format(util.addDays(tmpDay,6)));
                int _day = new Integer(Day.format(util.addDays(tmpDay,6)));
                String strWeek=(year*10000+month*100+day)+" To "+(_year*10000+_month*100+_day)+",";
                String strWeekNum=strWeek;

                for (int k=i;k<i+7;k++){//Check each week
                    //System.out.println(k);
                    for (int j=0; j<healthIndex.length;j++){ //Loop for each detector
                        detectorConfig.DetectorProperty tmpProperty = (detectorConfig.DetectorProperty) detConfigList.get(j);
                        String detour=tmpProperty.City;
                        if(detour.equals("Pasadena")){
                            if(healthIndex[j][k]==1)
                                numGoodPasadena=numGoodPasadena+1;
                            else if(healthIndex[j][k]==0)
                                numBadPasadena=numBadPasadena+1;
                            else
                                numNodataPasadena=numNodataPasadena+1;
                        }
                    }
                }
                strWeekNum=strWeekNum+Math.round(numGoodPasadena/7.0)+","+Math.round(numBadPasadena/7.0)+","+
                        Math.round(numNodataPasadena/7.0)+"\n";

                // Get the total numbers
                int totalPasadena=numGoodPasadena+numBadPasadena+numNodataPasadena;
                if(totalPasadena==0) //Avoid zeros
                    totalPasadena=1;

                strWeek=strWeek+(numGoodPasadena*100.0/totalPasadena)+","+(numBadPasadena*100.0/totalPasadena)+","+
                        (numNodataPasadena*100.0/totalPasadena)+"\n";
                bw.write(strWeek);
                bwNum.write(strWeekNum);

                tmpDay=util.addDays(tmpDay,7);
            }
            bw.close();
            bwNum.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void outputClusterDataToFile(int timestep,List<ClusterDetectorData> clusterDetectorDataList, File fileName) throws IOException {

        FileWriter fw = new FileWriter(fileName,true);
        BufferedWriter bw = new BufferedWriter(fw);
        String str;

        // Get the header
        if(timestep==0) {
            str = "Detector ID,Time,Count(#/5min), Volume(vph),Occupancy(%),Speed(??),Delay,Stops,Movement\n";
            bw.write(str);
        }

        for(int i=0;i<clusterDetectorDataList.size();i++){
            ClusterDetectorData clusterDetectorData=clusterDetectorDataList.get(i);
            str=clusterDetectorData.DetectorID+","+clusterDetectorData.Time+","+clusterDetectorData.Count+","+
                    clusterDetectorData.Volume+","+clusterDetectorData.Occupancy+","+clusterDetectorData.Speed+","+
                    clusterDetectorData.Delay+","+clusterDetectorData.Stops+","+clusterDetectorData.Movement+"\n";
            bw.write(str);
        }
        bw.close();
    }
}

