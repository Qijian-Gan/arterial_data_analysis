package loadData;

import java.io.*;
import java.util.*;

import config.detectorConfig;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * Created by Qijian-Gan on 9/20/2017.
 */
public  class loadTCSDetectorData {

    public static List loadTCSServerCSVData(File fileName){
        // This is the function to load TCS Server CSV Data
        List<loadTCSDetectorData.DetectorRawDataProfile> detRawDataList= new ArrayList<loadTCSDetectorData.DetectorRawDataProfile>();
        if(!fileName.exists())
        {
            System.out.println("Can not find the detector file!");
            return null;
        }
        else{
            System.out.println("Loading:"+fileName);
        }
        try {
            FileReader fr= new FileReader(fileName);
            BufferedReader br = new BufferedReader(fr);
            String line;
            line = br.readLine(); //Ignore the first line: header
            while ((line = br.readLine()) != null) { // Loop for the following rows
                String [] dataRow= line.split(",");
                // Get the detector ID
                int DetectorID=Integer.parseInt(dataRow[0]);
                //Get the date and time
                String dateTimeString=dataRow[1];
                List<Integer> DateTime=parseDateTimeString(dateTimeString);
                int Date=DateTime.get(0);
                int Time=DateTime.get(1);
                // Get the measurements
                double _Volume=Double.parseDouble(dataRow[3]);
                double _Occupancy=Double.parseDouble(dataRow[4]);
                double _Speed=Double.parseDouble(dataRow[5]);
                double _Delay=Double.parseDouble(dataRow[6]);
                double _Stops=Double.parseDouble(dataRow[7]);
                double _S_Volume=Double.parseDouble(dataRow[8]);
                double _S_Occupancy=Double.parseDouble(dataRow[9]);
                double _S_Speed=Double.parseDouble(dataRow[10]);
                double _S_Delay=Double.parseDouble(dataRow[11]);
                double _S_Stops=Double.parseDouble(dataRow[12]);
                DetectorRawDataProfile tmpDetectorData = new DetectorRawDataProfile(DetectorID, Date, Time,_Volume,_Occupancy,_Speed,
                        _Delay,_Stops,_S_Volume,_S_Occupancy,_S_Speed,_S_Delay,_S_Stops);
                detRawDataList.add(tmpDetectorData);
            }
            br.close();
            fr.close();
        }catch (IOException e)
        {
            e.printStackTrace();
        }
        return detRawDataList;
    }

    public static List parseDateTimeString(String dateTimeString){
        // This function is used to parse the data and time string
        List<Integer> DateTime=new ArrayList<Integer>();
        String tmpDateTimeString=dateTimeString.replace("\"","");
        String [] tmpDateTime= tmpDateTimeString.split(" ");

        int Month = 0;
        if(tmpDateTime[1].equals("Jan"))
            Month=1;
        else if (tmpDateTime[1].equals("Feb"))
            Month=2;
        else if (tmpDateTime[1].equals("Mar"))
            Month=3;
        else if (tmpDateTime[1].equals("Apr"))
            Month=4;
        else if (tmpDateTime[1].equals("May"))
            Month=5;
        else if (tmpDateTime[1].equals("Jun"))
            Month=6;
        else if (tmpDateTime[1].equals("Jul"))
            Month=7;
        else if (tmpDateTime[1].equals("Aug"))
            Month=8;
        else if (tmpDateTime[1].equals("Sep"))
            Month=9;
        else if (tmpDateTime[1].equals("Oct"))
            Month=10;
        else if (tmpDateTime[1].equals("Nov"))
            Month=11;
        else if (tmpDateTime[1].equals("Dec"))
            Month=12;
        else
            System.out.println("Wrong format of month!");

        int Date=Integer.parseInt(tmpDateTime[5])*10000+Month*100+Integer.parseInt(tmpDateTime[2]);
        int Time=parseTimeStringToSecond(tmpDateTime[3]);
        DateTime.add(Date);
        DateTime.add(Time);
        return DateTime;
    }

    public static int parseTimeStringToSecond(String timeString){
        // This function is used to convert the time string into seconds
        int timeInSecond=0;
        String [] tmpTimeString=timeString.split(":");
        timeInSecond=Integer.parseInt(tmpTimeString[0])*3600+Integer.parseInt(tmpTimeString[1])*60+Integer.parseInt(tmpTimeString[2]);
        return timeInSecond;
    }

    public static class DetectorRawDataProfile{
        // This is the profile for the TCS raw data
        public DetectorRawDataProfile(int _DetectorID, int _Date, int _Time, double _Volume, double _Occupany, double _Speed, double _Delay, double _Stops,
                                      double _S_Volume, double _S_Occupancy, double _S_Speed, double _S_Delay, double _S_Stops){

            this.DetectorID=_DetectorID;
            this.Date=_Date;
            this.Time=_Time;
            this.Volume=_Volume;
            this.Occupancy=_Occupany;
            this.Speed=_Speed;
            this.Delay=_Delay;
            this.Stops=_Stops;
            this.S_Volume=_S_Volume;
            this.S_Occupancy=_S_Occupancy;
            this.S_Speed=_S_Speed;
            this.S_Delay=_S_Delay;
            this.S_Stops=_S_Stops;
        }
        public int DetectorID;
        public int Date;
        public int Time;
        public double Volume;
        public double Occupancy;
        public double Speed;
        public double Delay;
        public double Stops;
        public double S_Volume;
        public double S_Occupancy;
        public double S_Speed;
        public double S_Delay;
        public double S_Stops;
    }


    public static class DetectorProcessedDataProfile
    {
        public DetectorProcessedDataProfile(int _DetectorID, String _Date, String _Time, double _Volume, double _Occupany, double _Speed){

            this.DetectorID=_DetectorID;
            this.Date=_Date;
            this.Time=_Time;
            this.Volume=_Volume;
            this.Occupancy=_Occupany;
            this.Speed=_Speed;
        }

        public int DetectorID;
        public String Date;
        public String Time;
        public double Volume;
        public double Occupancy;
        public double Speed;

    }
}
