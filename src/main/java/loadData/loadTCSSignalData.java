package loadData;

//import jdk.nashorn.internal.runtime.Timing;

import java.util.ArrayList;
import java.util.List;
import java.io.*;

/**
 * Created by Qijian-Gan on 11/20/2017.
 */
public class loadTCSSignalData {

    public static class PhaseTimes{
        // This is the profile of phase times
        public PhaseTimes(int _IntersectionID,int _PlanID, int _EndDate, int _EndTime, String _TimingStatus, int _CommFailures, int _OperationFailures,
                          int _DesiredCycleLength, int _ActualCycleLength, int _DesiredOffset, int _ActualOffset, String _PhaseMaxGreenTime,
                          String _PhaseActualGreenTime, String _PhasePedCalls, String _PhaseIsCoordinated){
            this.IntersectionID=_IntersectionID;
            this.PlanID=_PlanID;
            this.EndDate=_EndDate;
            this.EndTime=_EndTime;
            this.TimingStatus=_TimingStatus;
            this.CommFailures=_CommFailures;
            this.OperationFailures=_OperationFailures;
            this.DesiredCycleLength=_DesiredCycleLength;
            this.ActualCycleLength=_ActualCycleLength;
            this.DesiredOffset=_DesiredOffset;
            this.ActualOffset=_ActualOffset;
            this.PhaseMaxGreenTime=_PhaseMaxGreenTime;
            this.PhaseActualGreenTime=_PhaseActualGreenTime;
            this.PhasePedCalls=_PhasePedCalls;
            this.PhaseIsCoordinated=_PhaseIsCoordinated;
        }
        public int IntersectionID;
        public int PlanID;
        public int EndDate;
        public int EndTime;
        public String TimingStatus;
        public int CommFailures;
        public int OperationFailures;
        public int DesiredCycleLength;
        public int ActualCycleLength;
        public int DesiredOffset;
        public int ActualOffset;
        public String PhaseMaxGreenTime;
        public String PhaseActualGreenTime;
        public String PhasePedCalls;
        public String PhaseIsCoordinated;
    }

    public static class TimingPlans{
        // This is the profile for timing plans
        public TimingPlans(int _IntersectionID, int _PlanID,int _StartDate,int _StartTime,int _EndDate,int _EndTime,
                           String _PhaseAverageGreenTimePercent,String _PhaseInstanceMaxGreen, String _PhasePedCalls){
            this.IntersectionID=_IntersectionID;
            this.PlanID=_PlanID;
            this.StartDate=_StartDate;
            this.StartTime=_StartTime;
            this.EndDate=_EndDate;
            this.EndTime=_EndTime;
            this.PhaseAverageGreenTimePercent=_PhaseAverageGreenTimePercent;
            this.PhaseInstanceMaxGreen=_PhaseInstanceMaxGreen;
            this.PhasePedCalls=_PhasePedCalls;
        }
        public int IntersectionID;
        public int PlanID;
        public int StartDate;
        public int StartTime;
        public int EndDate;
        public int EndTime;
        public String PhaseAverageGreenTimePercent;
        public String PhaseInstanceMaxGreen;
        public String PhasePedCalls;
    }

    public static List<PhaseTimes> loadPhaseTimes(File fileName){
        List<PhaseTimes> phaseTimesList=new ArrayList<PhaseTimes>();

        if(!fileName.exists())
        {
            System.out.println("Can not find the signal phase time file!");
            return null;
        }
        else{
            System.out.println("Loading:"+fileName);
        }

        try {
            FileReader fr = new FileReader(fileName);
            BufferedReader br = new BufferedReader(fr);
            String[] fileNameStr = fileName.getName().split(".csv");
            fileNameStr = fileNameStr[0].split("-");
            int IntersectionID = Integer.parseInt(fileNameStr[1].trim());
            int StartSunday = Integer.parseInt(fileNameStr[2].trim()) * 10000 + Integer.parseInt(fileNameStr[3].trim()) * 100 +
                    Integer.parseInt(fileNameStr[4].trim());

            if (StartSunday > 20170723) { // Different date-time format before this date
                String line;
                line = br.readLine(); // Ignore the first line
                String[] Header = line.split(",");
                while ((line = br.readLine()) != null) { // Loop for the following rows
                    if (!line.contains("Cycle Number")) { // There may be multiple headers
                        String[] tmpString = line.split(",");

                        // Get the Date and Time
                        tmpString[1] = tmpString[1].replace("\"", "");
                        String[] DateTimeStr = tmpString[1].split(" ");
                        String[] DateStr = DateTimeStr[0].split("-");
                        int Date = Integer.parseInt(DateStr[0].trim()) * 10000 + Integer.parseInt(DateStr[1].trim()) * 100 + Integer.parseInt(DateStr[2].trim());
                        String[] TimeStr = DateTimeStr[1].split(":");
                        int Time = Integer.parseInt(TimeStr[0].trim()) * 3600 + Integer.parseInt(TimeStr[1].trim()) * 60 + Integer.parseInt(TimeStr[2].trim());

                        int PlanID = Integer.parseInt(tmpString[2]);
                        String TimingStatus = tmpString[3];
                        int CommFailures = Integer.parseInt(tmpString[4]);
                        int OperationFailures = Integer.parseInt(tmpString[5]);
                        int DesiredCycleLength = Integer.parseInt(tmpString[6]);
                        int ActualCycleLength = Integer.parseInt(tmpString[7]);
                        int DesiredOffset = Integer.parseInt(tmpString[8]);
                        int ActualOffset = Integer.parseInt(tmpString[9]);

                        String PhaseMaxGreenTime = "";
                        String PhaseActualGreenTime = "";
                        String PhasePedCalls = "";
                        String PhaseIsCoordinated = "";
                        for (int i = 10; i < tmpString.length; i++) {
                            if (Header[i].contains("Max Green Time")) {
                                if (PhaseMaxGreenTime.equals(""))
                                    PhaseMaxGreenTime = tmpString[i] + ";";
                                else
                                    PhaseMaxGreenTime = PhaseMaxGreenTime + tmpString[i] + ";";
                            }
                            if (Header[i].contains("Actual Green Time")) {
                                if (PhaseActualGreenTime.equals(""))
                                    PhaseActualGreenTime = tmpString[i] + ";";
                                else
                                    PhaseActualGreenTime = PhaseActualGreenTime + tmpString[i] + ";";
                            }
                            if (Header[i].contains("Ped Calls")) {
                                if (PhasePedCalls.equals(""))
                                    PhasePedCalls = tmpString[i] + ";";
                                else
                                    PhasePedCalls = PhasePedCalls + tmpString[i] + ";";
                            }
                            if (Header[i].contains("Is Coordinated")) {
                                if (PhaseIsCoordinated.equals(""))
                                    PhaseIsCoordinated = tmpString[i] + ";";
                                else
                                    PhaseIsCoordinated = PhaseIsCoordinated + tmpString[i] + ";";
                            }
                        }

                        PhaseTimes phaseTimes = new PhaseTimes(IntersectionID, PlanID, Date, Time, TimingStatus, CommFailures, OperationFailures,
                                DesiredCycleLength, ActualCycleLength, DesiredOffset, ActualOffset, PhaseMaxGreenTime,
                                PhaseActualGreenTime, PhasePedCalls, PhaseIsCoordinated);
                        phaseTimesList.add(phaseTimes);
                    }
                }
            }
            br.close();
            fr.close();
        }catch (IOException e)
        {
            e.printStackTrace();
        }

        return phaseTimesList;
    }

    public static List<TimingPlans> loadTimingPlans(File fileName){
        List<TimingPlans> timingPlansList=new ArrayList<TimingPlans>();

        if(!fileName.exists())
        {
            System.out.println("Can not find the signal timing plan file!");
            return null;
        }
        else{
            System.out.println("Loading:"+fileName);
        }

        try {
            FileReader fr= new FileReader(fileName);
            BufferedReader br = new BufferedReader(fr);
            String[] fileNameStr = fileName.getName().split(".csv");
            fileNameStr = fileNameStr[0].split("-");
            int IntersectionID = Integer.parseInt(fileNameStr[1].trim());
            int StartSunday = Integer.parseInt(fileNameStr[2].trim()) * 10000 + Integer.parseInt(fileNameStr[3].trim()) * 100 +
                    Integer.parseInt(fileNameStr[4].trim());

            if (StartSunday > 20170723) { // Different date-time format before this date
                String line;
                line = br.readLine(); // Ignore the first line
                String[] Header = line.split(",");
                while ((line = br.readLine()) != null) { // Loop for the following rows
                    if (!line.contains("Plan")) { // There may be multiple headers
                        String[] tmpString = line.split(",");

                        int PlanID = Integer.parseInt(tmpString[0]);

                        // Get the starting Date and Time
                        tmpString[1] = tmpString[1].replace("\"", "");
                        String[] DateTimeStr = tmpString[1].split(" ");
                        String[] DateStr = DateTimeStr[0].split("-");
                        int StartDate = Integer.parseInt(DateStr[0].trim()) * 10000 + Integer.parseInt(DateStr[1].trim()) * 100 + Integer.parseInt(DateStr[2].trim());
                        String[] TimeStr = DateTimeStr[1].split(":");
                        int StartTime = Integer.parseInt(TimeStr[0].trim()) * 3600 + Integer.parseInt(TimeStr[1].trim()) * 60 + Integer.parseInt(TimeStr[2].trim());
                        // Get the ending Date and Time
                        tmpString[2] = tmpString[2].replace("\"", "");
                        DateTimeStr = tmpString[2].split(" ");
                        DateStr = DateTimeStr[0].split("-");
                        int EndDate = Integer.parseInt(DateStr[0].trim()) * 10000 + Integer.parseInt(DateStr[1].trim()) * 100 + Integer.parseInt(DateStr[2].trim());
                        TimeStr = DateTimeStr[1].split(":");
                        int EndTime = Integer.parseInt(TimeStr[0].trim()) * 3600 + Integer.parseInt(TimeStr[1].trim()) * 60 + Integer.parseInt(TimeStr[2].trim());

                        String PhaseAverageGreenTimePercent = "";
                        String PhaseInstanceMaxGreen = "";
                        String PhasePedCalls = "";
                        for (int i = 3; i < tmpString.length; i++) {
                            if (Header[i].contains("Average Green Time (%)")) {
                                if (PhaseAverageGreenTimePercent.equals(""))
                                    PhaseAverageGreenTimePercent = tmpString[i] + ";";
                                else
                                    PhaseAverageGreenTimePercent = PhaseAverageGreenTimePercent + tmpString[i] + ";";
                            }
                            if (Header[i].contains("Instances of Max Green")) {
                                if (PhaseInstanceMaxGreen.equals(""))
                                    PhaseInstanceMaxGreen = tmpString[i] + ";";
                                else
                                    PhaseInstanceMaxGreen = PhaseInstanceMaxGreen + tmpString[i] + ";";
                            }
                            if (Header[i].contains("Ped Calls")) {
                                if (PhasePedCalls.equals(""))
                                    PhasePedCalls = tmpString[i] + ";";
                                else
                                    PhasePedCalls = PhasePedCalls + tmpString[i] + ";";
                            }
                        }

                        TimingPlans timingPlans = new TimingPlans(IntersectionID, PlanID, StartDate, StartTime, EndDate, EndTime,
                                PhaseAverageGreenTimePercent, PhaseInstanceMaxGreen, PhasePedCalls);
                        timingPlansList.add(timingPlans);
                    }
                }
            }
            br.close();
            fr.close();

        }catch (IOException e)
        {
            e.printStackTrace();
        }

        return timingPlansList;
    }
}
