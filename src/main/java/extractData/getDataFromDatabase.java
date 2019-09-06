package extractData;

import config.detectorConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static Utility.util.*;
import config.detectorConfig.DetectorProperty;
import sun.java2d.pipe.SpanShapeRenderer;

public class getDataFromDatabase {

    public static class ClusterDetectorData{
        public int DetectorID;
        public String Time;
        public double Count;
        public double Volume;
        public double Occupancy;
        public double Speed;
        public double Delay;
        public double Stops;
        public String Movement;

        public ClusterDetectorData(int _DetectorID, String _Time, double _Count, double _Volume, double _Occupancy,
                                   double _Speed, double _Delay, double _Stops, String _Movement){
            DetectorID=_DetectorID;
            Time=_Time;
            Count=_Count;
            Volume=_Volume;
            Occupancy=_Occupancy;
            Speed=_Speed;
            Delay=_Delay;
            Stops=_Stops;
            Movement=_Movement;
        }
    }

    public static List<ClusterDetectorData> getClusteredDataForAllDetectorsForGivenTime(Connection con, String dataTableName
            ,String healthTableName,List<DetectorProperty> detectorPropertyList,int selectYear
            ,boolean useMedian,int timeStamp,int interval, String clusterSetting) throws SQLException, ParseException {

        List<ClusterDetectorData> clusterDetectorDataList=new ArrayList<ClusterDetectorData>();

        for(int i=0;i<detectorPropertyList.size();i++) {
            DetectorProperty detectorProperty=detectorPropertyList.get(i);
            int detId = detectorProperty.IntID * 100 + detectorProperty.SensorID;
            System.out.println("DetID="+detId+" & Time="+timeStamp);

            Map<String, Integer> selectedDate = null;
            selectedDate = getHealthDataForADetectorForGivenYear(con, healthTableName, detId, selectYear, clusterSetting);

            int hour = (timeStamp + interval) / 3600;
            int minute = (timeStamp + interval - hour * 3600) / 60;
            String timeStr;
            if (minute < 10) {
                timeStr = hour + ":0" + minute + ":00";
            } else {
                timeStr = hour + ":" + minute + ":00";
            }
            if(hour==24){
                // End of day, Aimsun uses 00:00:00 to stand for 24:00:00
                timeStr="0:00:00";
            }

            double[] clusterDataByTime = getClusteredDataForADetectorForAGivenTimeStamp(con, dataTableName, detId
                    , selectYear, selectedDate, timeStamp, useMedian);

            if (clusterDataByTime != null)
            {
                ClusterDetectorData clusterDetectorData = new ClusterDetectorData(detId, timeStr, clusterDataByTime[0] / (3600 / interval)
                        , clusterDataByTime[0], clusterDataByTime[1] / 3600 * 100, clusterDataByTime[2], 0, 0
                        , detectorProperty.Movement);
                clusterDetectorDataList.add(clusterDetectorData);
            }
        }
        System.gc();
        return clusterDetectorDataList;
    }

    public static Map<String,Integer> getHealthDataForADetectorForGivenYear(Connection con, String healthTableName, int detId
            , int selectYear,String clusterSetting) throws SQLException, ParseException {

        Map<String,Integer> healthData=new HashMap<String, Integer>();

        String query = "select Year,Month,Day,Health from "+ healthTableName+" where DetectorID=" + detId
                + " and Year=" + selectYear + ";";

        Statement sqlStatement = con.createStatement();

        ResultSet result = sqlStatement.executeQuery(query);
        while (result.next()) {
            int year=result.getInt("Year");
            int month=result.getInt("Month");
            int day=result.getInt("Day");
            int health=result.getInt("Health");
            if(checkClusterSetting(clusterSetting,year,month,day)){
                healthData.put(year+"-"+month+"-"+day,health);
            }
        }
        result.close();
        sqlStatement.close();
        return healthData;
    }

    public static boolean checkClusterSetting(String clusterSetting,int year,int month, int day) throws ParseException {

        boolean satisfy=true;

        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd");
        Date date=simpleDateFormat.parse(year+"-"+month+"-"+day);
        Calendar calendar=Calendar.getInstance();
        calendar.setTime(date);

        // Sunday:1... Saturday:7
        int dayOfWeek=calendar.get(Calendar.DAY_OF_WEEK);
        if(clusterSetting.equals("Sunday")){
            if(dayOfWeek!=1){
                satisfy=false;
            }
        }else if(clusterSetting.equals("Monday")){
            if(dayOfWeek!=2){
                satisfy=false;
            }
        }else if(clusterSetting.equals("Tuesday")){
            if(dayOfWeek!=3){
                satisfy=false;
            }
        }
        else if(clusterSetting.equals("Wednesday")){
            if(dayOfWeek!=4){
                satisfy=false;
            }
        }
        else if(clusterSetting.equals("Thursday")){
            if(dayOfWeek!=5){
                satisfy=false;
            }
        }
        else if(clusterSetting.equals("Friday")){
            if(dayOfWeek!=6){
                satisfy=false;
            }
        }
        else if(clusterSetting.equals("Saturday")){
            if(dayOfWeek!=7){
                satisfy=false;
            }
        }
        else if(clusterSetting.equals("Weekday")){
            if(dayOfWeek==1 || dayOfWeek==7){
                satisfy=false;
            }
        }else if(clusterSetting.equals("Weekend")){
            if(dayOfWeek>1 && dayOfWeek<7){
                satisfy=false;
            }
        }else{
            ;
        }
        return satisfy;
    }


    public static double[] getClusteredDataForADetectorForAGivenTimeStamp(Connection con,String dataTableName,int detId
            ,int selectYear, Map<String,Integer> selectDate,int timeStamp,boolean useMedian) throws SQLException {
        double [] clusterData=null;

        String query = "select * from "+ dataTableName+" where DetectorID=" + detId
                + " and Year=" + selectYear + " and Time="+timeStamp+";";

        List<double[]> selectedData=new ArrayList<double[]>();
        Statement sqlStatement = con.createStatement();
        ResultSet result = sqlStatement.executeQuery(query);
        while (result.next()) {
            int year=result.getInt("Year");
            int month=result.getInt("Month");
            int day=result.getInt("Day");
            if(selectDate.containsKey(year+"-"+month+"-"+day)){
                if(selectDate.get(year+"-"+month+"-"+day)==1){
                    double volume=result.getDouble("Volume");
                    double occupancy=result.getDouble("Occupancy");
                    double speed=result.getDouble("Speed");
                    selectedData.add(new double[]{volume,occupancy,speed});
                }
            }
        }

        if(selectedData.size()>0) {
            double[][] selectedDataMatrix = convertArrayToMatrix(selectedData);
            double[] tmpVolume=fromMatrixToArrayByColumn(selectedDataMatrix,0, 0, selectedDataMatrix.length-1);
            double[] tmpOcc=fromMatrixToArrayByColumn(selectedDataMatrix,1, 0, selectedDataMatrix.length-1);
            double[] tmpSpeed=fromMatrixToArrayByColumn(selectedDataMatrix,2, 0, selectedDataMatrix.length-1);
            if(useMedian){
                double medianVolume=calculateMedian(tmpVolume);
                double medianOcc=calculateMedian(tmpOcc);
                double medianSpeed=calculateMedian(tmpSpeed);
                clusterData=new double[]{medianVolume,medianOcc,medianSpeed};
            }else{
                double meanVolume=calculateMean(tmpVolume);
                double meanOcc=calculateMean(tmpOcc);
                double meanSpeed=calculateMean(tmpSpeed);
                clusterData=new double[]{meanVolume,meanOcc,meanSpeed};
            }
        }

        sqlStatement.close();
        result.close();
        return clusterData;
    }
}
