package ATSPMs;

import Utility.*;
import java.awt.Color;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.*;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.ui.ApplicationFrame;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.renderer.xy.XYItemRenderer;

/**
 * Created by Qijian-Gan on 11/21/2017.
 */
public class calculateATSMPs {
    // This is the class for ATSPMs measurements on traffic signal phasing data

    //******************** Parameters **********************************
    public static class PhaseTimeList{
        public PhaseTimeList(List<Integer> _PlanIDList,List<Integer> _EndTimeList,List<String> _TimingStatusList,
                             List<Integer> _DesiredCycleLengthList,List<Integer> _ActualCycleLengthList,
                             List<Integer> _DesiredOffsetList,List<Integer> _ActualOffsetList,
                             List<List<Integer>> _PhaseMaxGreenTimeList, List<List<Integer>> _PhaseActualGreenTimeList,
                             List<List<Integer>> _PhasePedCallsList, List<List<String>> _PhaseCoordinatedList){
            this.PlanIDList=_PlanIDList;
            this.EndTimeList=_EndTimeList;
            this.TimingStatusList=_TimingStatusList;
            this.DesiredCycleLengthList=_DesiredCycleLengthList;
            this.ActualCycleLengthList=_ActualCycleLengthList;
            this.DesiredOffsetList=_DesiredOffsetList;
            this.ActualOffsetList=_ActualOffsetList;
            this.PhaseMaxGreenTimeList=_PhaseMaxGreenTimeList;
            this.PhaseActualGreenTimeList=_PhaseActualGreenTimeList;
            this.PhasePedCallsList=_PhasePedCallsList;
            this.PhaseCoordinatedList=_PhaseCoordinatedList;
        }
        public List<Integer> PlanIDList;
        public List<Integer> EndTimeList;
        public List<String> TimingStatusList;
        public List<Integer> DesiredCycleLengthList;
        public List<Integer> ActualCycleLengthList;
        public List<Integer> DesiredOffsetList;
        public List<Integer> ActualOffsetList;
        public List<List<Integer>> PhaseMaxGreenTimeList;
        public List<List<Integer>> PhaseActualGreenTimeList;
        public List<List<Integer>> PhasePedCallsList;
        public List<List<String>> PhaseCoordinatedList;
    }

    public static class IntersectionProperty{
        // This is the intersection property
        public IntersectionProperty(String _IntersectionName,int _IntersectionID, String _Direction, int _SensorID,
                                    String _Movement,int _NumOfLanes){
            this.IntersectionID=_IntersectionID;
            this.IntersectionName=_IntersectionName;
            this.Direction=_Direction;
            this.SensorID=_SensorID;
            this.Movement=_Movement;
            this.NumOfLanes=_NumOfLanes;
        }
        public String IntersectionName;
        public int IntersectionID;
        public String Direction;
        public int SensorID;
        public String Movement;
        public int NumOfLanes;
    }

    public static PhaseTimeList getSQLData(ResultSet Result){
        // This function is used to get SQL data
        List<Integer> PlanIDList=new ArrayList<Integer>();
        List<Integer> EndTimeList =new ArrayList<Integer>();
        List<String> TimingStatusList =new ArrayList<String>();
        List<Integer> DesiredCycleLengthList=new ArrayList<Integer>();
        List<Integer> ActualCycleLengthList=new ArrayList<Integer>();
        List<Integer> DesiredOffsetList=new ArrayList<Integer>();
        List<Integer> ActualOffsetList=new ArrayList<Integer>();
        List<List<Integer>> PhaseMaxGreenTimeList=new ArrayList<List<Integer>>();
        List<List<Integer>> PhaseActualGreenTimeList=new ArrayList<List<Integer>>();
        List<List<Integer>> PhasePedCallsList=new ArrayList<List<Integer>>();
        List<List<String>> PhaseCoordinatedList=new ArrayList<List<String>>();

        try {
            while (Result.next()) {
                PlanIDList.add(Result.getInt("PlanID"));
                EndTimeList.add(Result.getInt("EndTime"));
                TimingStatusList.add(Result.getString("TimingStatus"));
                DesiredCycleLengthList.add(Result.getInt("DesiredCycleLength"));
                ActualCycleLengthList.add(Result.getInt("ActualCycleLength"));
                DesiredOffsetList.add(Result.getInt("DesiredOffset"));
                ActualOffsetList.add(Result.getInt("ActualOffset"));

                String MaxGreenTimeStr=Result.getString("PhaseMaxGreenTime");
                List<Integer> MaxGreenTime=parseStringToInteger(MaxGreenTimeStr);
                PhaseMaxGreenTimeList.add(MaxGreenTime);

                String ActualGreenTimeStr=Result.getString("PhaseActualGreenTime");
                List<Integer> ActualGreenTime=parseStringToInteger(ActualGreenTimeStr);
                PhaseActualGreenTimeList.add(ActualGreenTime);

                String PedCallsStr=Result.getString("PhasePedCalls");
                List<Integer> PedCalls=parseStringToInteger(PedCallsStr);
                PhasePedCallsList.add(PedCalls);

                String IsCoordinatedStr=Result.getString("PhaseIsCoordinated");
                List<String> IsCoordinated=parseStringToString(IsCoordinatedStr);
                PhaseCoordinatedList.add(IsCoordinated);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        PhaseTimeList phaseTimeList=new PhaseTimeList(PlanIDList,EndTimeList,TimingStatusList,
                DesiredCycleLengthList,ActualCycleLengthList,DesiredOffsetList,ActualOffsetList,
                PhaseMaxGreenTimeList,PhaseActualGreenTimeList,PhasePedCallsList,PhaseCoordinatedList);
        return phaseTimeList;
    }

    public static List<IntersectionProperty> getIntersectionProperty(ResultSet Result){
        // This function is used to get intersection property
        List<IntersectionProperty> intersectionPropertyList=new ArrayList<IntersectionProperty>();
        try {
            while (Result.next()) {
                String IntersectionName=Result.getString("IntersectionName");
                int IntersectionID=Result.getInt("IntersectionID");
                String Direction=Result.getString("Direction");
                int SensorID=Result.getInt("SensorID");
                String Movement=Result.getString("Movement");
                int NumOfLanes=Result.getInt("NumOfLanes");

                IntersectionProperty intersectionProperty=new IntersectionProperty(IntersectionName,IntersectionID,Direction,SensorID,
                        Movement,NumOfLanes);
                intersectionPropertyList.add(intersectionProperty);
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return intersectionPropertyList;
    }

    public static List<Integer> parseStringToInteger(String InputStr){
        // This function is used to parse string to integer
        List<Integer> OutputInt=new ArrayList<Integer>();
        String [] StringList=InputStr.split(";");
        for(int i=0;i<StringList.length;i++){
            OutputInt.add(Integer.parseInt(StringList[i].trim()));
        }
        return OutputInt;
    }

    public static List<String> parseStringToString(String InputStr){
        // This function is used to parse string to string list
        List<String> OutputStr=new ArrayList<String>();
        String [] StringList=InputStr.split(";");
        for(int i=0;i<StringList.length;i++){
            OutputStr.add(StringList[i].trim());
        }
        return OutputStr;
    }

    //******************** Major functions **********************************
    public static void drawPedestrianCalls(PhaseTimeList phaseTimeList,String Type, int BinSizeInSecond){
        // This function is used to draw Pedestrian  Calls by each phase

        int NumPhases=phaseTimeList.PhasePedCallsList.get(0).size(); // Get Number of Phases
        int [][] PedCalls=convertListToMatrixInteger(phaseTimeList.PhasePedCallsList); // Get Matrix: #Observation * #Phases
        int [] EndTimes=convertListToArrayInteger(phaseTimeList.EndTimeList); // Get the array of EndTime

        Set<String> UniqueTimingStatus=new HashSet<String>(phaseTimeList.TimingStatusList);

        if(Type.equals("RawData")) { // If only plot raw data
            for (int i = 0; i < NumPhases; i++) { // Loop for each phase
                XYSeriesCollection DataSet = new XYSeriesCollection();

                int[] YMeasure = getColumnFromMatrix(PedCalls, i);
                if (util.getTheSumInteger(YMeasure) > 0) {
                    Iterator<String> tmp=UniqueTimingStatus.iterator();
                    while(tmp.hasNext()) {
                        final XYSeries series = getXTimeYMeasureByTimingStatus(EndTimes, YMeasure, phaseTimeList.TimingStatusList,
                                tmp.next());
                        DataSet.addSeries(series);
                    }
                    JFreeChart demo = ChartFactory.createScatterPlot("Pedestrian Calls In Phase:"+(i+1), "Time (Hour of Day)",
                            "Number of Calls", DataSet,PlotOrientation.VERTICAL,true, true, false);
                    final ChartPanel chartPanel = new ChartPanel(demo);
                    chartPanel.setPreferredSize(new java.awt.Dimension(1000, 500));
                    final ApplicationFrame frame = new ApplicationFrame("Title");
                    frame.setContentPane(chartPanel);
                    frame.pack();
                    frame.setVisible(true);
                }
            }
        }

        if(Type.equals("ByFixedInterval")){
            for (int i = 0; i < NumPhases; i++) { // Loop for each phase
                HistogramDataset DataSet = new HistogramDataset();
                int[] YMeasure = getColumnFromMatrix(PedCalls, i);
                if (util.getTheSumInteger(YMeasure) > 0) {
                    Iterator<String> tmp=UniqueTimingStatus.iterator();
                    while(tmp.hasNext()) {
                        String TimingStatus=tmp.next();
                        final double[] YSeries= getYMeasureByTimingStatusByBin(EndTimes, YMeasure, phaseTimeList.TimingStatusList,
                                TimingStatus,BinSizeInSecond);
                        if(YSeries.length>0) {
                            DataSet.addSeries(TimingStatus, YSeries, 24 * 3600 / BinSizeInSecond,0,24);
                        }else{
                            double [] fakeValues={-1.0};
                            DataSet.addSeries(TimingStatus,fakeValues,24 * 3600 / BinSizeInSecond,0,24);
                        }
                    }
                    JFreeChart demo = ChartFactory.createHistogram("Pedestrian Calls In Phase:"+(i+1), "Time (Hour of Day)",
                            "Number of Calls", DataSet,PlotOrientation.VERTICAL,true, false, false);
                    ChartFactory.setChartTheme(StandardChartTheme.createJFreeTheme());
                    final ChartPanel chartPanel = new ChartPanel(demo);
                    chartPanel.setPreferredSize(new java.awt.Dimension(1000, 500));
                    final ApplicationFrame frame = new ApplicationFrame("Pedestrian Calls");
                    frame.setContentPane(chartPanel);
                    frame.pack();
                    frame.setVisible(true);
                }
            }

        }
    }

    public static void drawSplitMonitor(PhaseTimeList phaseTimeList){
        // This function is used to draw split monitor

        int NumPhases=phaseTimeList.PhasePedCallsList.get(0).size(); // Get Number of Phases
        int [][] PhaseActualGreenTimes=convertListToMatrixInteger(phaseTimeList.PhaseActualGreenTimeList); // Get Matrix: #Observation * #Phases
        int [][] PhasePlannedGreenTimes=convertListToMatrixInteger(phaseTimeList.PhaseMaxGreenTimeList); // Get Matrix: #Observation * #Phases
        int [] EndTimes=convertListToArrayInteger(phaseTimeList.EndTimeList); // Get the array of EndTime

        Set<String> UniqueTimingStatus;

        for (int i = 0; i < NumPhases; i++) { // Loop for each phase
            XYSeriesCollection DataSet = new XYSeriesCollection();

            int[] YMeasure = getColumnFromMatrix(PhaseActualGreenTimes, i);
            int[] YMeasureMax=getColumnFromMatrix(PhasePlannedGreenTimes, i);

            if (util.getTheSumInteger(YMeasure) > 0) {
                // Add the planned time series: only for "Coordinated" mode
                UniqueTimingStatus=new HashSet<String>(phaseTimeList.TimingStatusList);
                Iterator<String> tmp=UniqueTimingStatus.iterator();
                while(tmp.hasNext()) {
                    String Mode=tmp.next();// Get the mode
                    final XYSeries series = getXTimeYMeasureByTimingStatus(EndTimes, YMeasure, phaseTimeList.TimingStatusList,Mode);
                    DataSet.addSeries(series);// Add the series: actual values

                    if(Mode.equals("COORDINATED")){ // For coordinated cases
                        final XYSeries seriesMax = getXTimeYMeasureByTimingStatus(EndTimes, YMeasureMax, phaseTimeList.TimingStatusList, Mode);
                        seriesMax.setKey("MAX GREEN-COORDINATED");// Get the max values
                        // For the coordinated phase, seriesMax would be zeros
                        DataSet.addSeries(seriesMax);
                        /*
                        if(seriesMax.getMaxY()==0){
                            // Get the new series
                            double PlannedGreen=getPlannedGreenByPercentile(series,0.01);
                            XYSeries seriesMaxNew=new XYSeries("MAX GREEN-COORDINATED");
                            for(int k=0;k<seriesMax.getItemCount();k++){
                                seriesMaxNew.add(seriesMax.getX(k),PlannedGreen);
                            }
                            DataSet.addSeries(seriesMaxNew);
                        }else{
                            DataSet.addSeries(seriesMax);
                        }
                        */
                    }
                }

                JFreeChart demo = ChartFactory.createScatterPlot("Split Monitor In Phase:"+(i+1), "Time (Hour of Day)",
                        "Duration (Second)", DataSet,PlotOrientation.VERTICAL,true, true, false);
                final ChartPanel chartPanel = new ChartPanel(demo);
                chartPanel.setPreferredSize(new java.awt.Dimension(1000, 500));
                final ApplicationFrame frame = new ApplicationFrame("Split Monitor");
                frame.setContentPane(chartPanel);
                frame.pack();
                frame.setVisible(true);

                XYPlot xyPlot = (XYPlot) demo.getPlot();
                xyPlot.setDomainCrosshairVisible(true);
                xyPlot.setRangeCrosshairVisible(true);
                XYItemRenderer renderer = xyPlot.getRenderer();
                renderer.setSeriesPaint(0, Color.blue);
                NumberAxis domain = (NumberAxis) xyPlot.getDomainAxis();
                domain.setRange(0.00, 24.00);
                NumberAxis range = (NumberAxis) xyPlot.getRangeAxis();
                range.setRange(0.0, 120.0);
            }
        }
    }

    public static void drawTurningMovementCounts(List<IntersectionProperty> intersectionPropertyList, Statement ps,int Date, int Interval){
        // This function is used to draw turning movement counts by approach/direction

        // Get available approaches/directions
        String IntersectionName=intersectionPropertyList.get(0).IntersectionName;
        List<String> ApproachStrings=new ArrayList<String>();
        for(int i=0;i<intersectionPropertyList.size();i++){
            ApproachStrings.add(intersectionPropertyList.get(i).Direction);
        }
        Set<String> UniqueApproaches=new HashSet<String>(ApproachStrings);
        Iterator<String> tmp=UniqueApproaches.iterator();
        while(tmp.hasNext()){// Loop for each approach
            String CurApproach =tmp.next();
            // Check the existence of turning detectors
            List<IntersectionProperty> SelectedLeftTurnDetectors=new ArrayList<IntersectionProperty>();
            List<IntersectionProperty> SelectedRightTurnDetectors=new ArrayList<IntersectionProperty>();
            for (int i=0;i<intersectionPropertyList.size();i++){
                if(intersectionPropertyList.get(i).Direction.equals(CurApproach)){
                    // Get the list of left-turn detectors
                    if(intersectionPropertyList.get(i).Movement.equals("Left Turn")) {
                        SelectedLeftTurnDetectors.add(intersectionPropertyList.get(i));
                    }
                    // Get the list of right-turn detectors
                    if(intersectionPropertyList.get(i).Movement.equals("Right Turn")) {
                        SelectedRightTurnDetectors.add(intersectionPropertyList.get(i));
                    }
                }
            }

            if(!SelectedLeftTurnDetectors.isEmpty()){
                XYSeries totalLeftTurnCount=getTotalDetectorFlowCount(SelectedLeftTurnDetectors,ps,Date,Interval);
                new XYLineChart(IntersectionName+" : "+CurApproach+" : Left Turn","Time (Hour of Day)",
                        "Flow Counts (VPH)",  totalLeftTurnCount);
            }
            if(!SelectedRightTurnDetectors.isEmpty()){
                XYSeries totalRightTurnCount=getTotalDetectorFlowCount(SelectedRightTurnDetectors,ps,Date,Interval);
                new XYLineChart(IntersectionName+" : "+CurApproach+" : Right Turn","Time (Hour of Day)",
                        "Flow Counts (VPH)",  totalRightTurnCount);
            }
        }
    }

    public static void drawApproachFlowAndOccupancy(List<IntersectionProperty> intersectionPropertyList, Statement ps,int Date, int Interval, String Case){
        // This function is used to plot approach flow and occupancy

        // Get available approaches/directions
        String IntersectionName=intersectionPropertyList.get(0).IntersectionName;
        List<String> ApproachStrings=new ArrayList<String>();
        for(int i=0;i<intersectionPropertyList.size();i++){
            ApproachStrings.add(intersectionPropertyList.get(i).Direction);
        }
        Set<String> UniqueApproaches=new HashSet<String>(ApproachStrings);
        Iterator<String> tmp=UniqueApproaches.iterator();
        while(tmp.hasNext()){// Loop for each approach
            String CurApproach =tmp.next();
            // Check the existence of turning detectors
            List<IntersectionProperty> SelectedAdvancedDetectors=new ArrayList<IntersectionProperty>();
            for (int i=0;i<intersectionPropertyList.size();i++){
                if(intersectionPropertyList.get(i).Direction.equals(CurApproach)){
                    // Get the list of left-turn detectors
                    if(intersectionPropertyList.get(i).Movement.contains("Advanced")) {
                        SelectedAdvancedDetectors.add(intersectionPropertyList.get(i));
                    }
                }
            }
            if(!SelectedAdvancedDetectors.isEmpty()){
                if(Case.equals("Flow")) {
                    XYSeries totalApproachCount = getTotalDetectorFlowCount(SelectedAdvancedDetectors, ps, Date, Interval);
                    new XYLineChart(IntersectionName + " : " + CurApproach + " : Advanced Detectors", "Time (Hour of Day)",
                            "Flow Counts (VPH)", totalApproachCount);
                }
                else if(Case.equals("Occupancy")){
                    XYSeries avgApproachOccupancy = getAverageDetectorOccupancy(SelectedAdvancedDetectors, ps, Date, Interval);
                    new XYLineChart(IntersectionName + " : " + CurApproach + " : Advanced Detectors", "Time (Hour of Day)",
                            "Average Occupancy(%)", avgApproachOccupancy);
                }
                else if(Case.equals("Speed")){
                    XYSeries avgApproachSpeed = getAverageDetectorSpeed(SelectedAdvancedDetectors, ps, Date, Interval);
                    new XYLineChart(IntersectionName + " : " + CurApproach + " : Advanced Detectors", "Time (Hour of Day)",
                            "Average Speed(mph)", avgApproachSpeed);
                }else if(Case.equals("Delay")){
                    XYSeries avgApproachDelay = getAverageDetectorDelay(SelectedAdvancedDetectors, ps, Date, Interval);
                    new XYLineChart(IntersectionName + " : " + CurApproach + " : Advanced Detectors", "Time (Hour of Day)",
                            "Average Delay(Seconds/Vehicle)", avgApproachDelay);
                }else if(Case.equals("Time&Flow&Occupancy")){
                    XYSeries totalApproachCount = getTotalDetectorFlowCount(SelectedAdvancedDetectors, ps, Date, Interval);
                    XYSeries avgApproachOccupancy = getAverageDetectorOccupancy(SelectedAdvancedDetectors, ps, Date, Interval);
                    new XYYLineChart(IntersectionName + " : " + CurApproach + " : Advanced Detectors", "Time (Hour of Day)",
                            "Flow Counts (VPH)","Average Occupancy(%)", totalApproachCount,avgApproachOccupancy);
                }
                else if(Case.equals("Flow&Occupancy")){
                    XYSeries totalApproachCount = getTotalDetectorFlowCount(SelectedAdvancedDetectors, ps, Date, Interval);
                    XYSeries avgApproachOccupancy = getAverageDetectorOccupancy(SelectedAdvancedDetectors, ps, Date, Interval);
                    XYSeries xySeries=new XYSeries("Flow-Occupancy");
                    for(int i=0;i<totalApproachCount.getItemCount();i++) {
                        xySeries.add(avgApproachOccupancy.getY(i), totalApproachCount.getY(i));
                    }
                    new XYScatterPlot(IntersectionName + " : " + CurApproach + " : Advanced Detectors", "Average Occupancy(%)",
                            "Flow Counts (VPH)", xySeries);
                }
            }
        }
    }

    //******************** Other functions **********************************
    public static XYSeries getAverageDetectorDelay(List<IntersectionProperty> intersectionPropertyList,Statement ps,int Date, int Interval){
        // Get average detector Delay
        int NumInterval=24*3600/Interval;
        int Year=Date/10000;
        double [][] AvgDelay=new double[NumInterval][3];
        for(int i=0;i<NumInterval;i++) {
            AvgDelay[i][0]=i*Interval/3600.0;
            AvgDelay[i][1] = 0;
            AvgDelay[i][2] = 0.000001;
        }
        for(int i=0;i<intersectionPropertyList.size();i++){// Loop for each detector
            int DetectorID=intersectionPropertyList.get(i).IntersectionID*100+intersectionPropertyList.get(i).SensorID;
            int NumOfLanes=intersectionPropertyList.get(i).NumOfLanes;
            // Get the data
            String sql="Select Time, Volume, Delay from detector_data_raw_"+Year+" where Date="+Date+" and DetectorID="+DetectorID+";";
            try{
                ResultSet resultSet=ps.executeQuery(sql);
                while (resultSet.next()) {
                    int tmpTime=resultSet.getInt("Time");
                    int timeIndex=tmpTime/Interval;
                    // Add up the flow counts: lane-based flow * number of lanes
                    AvgDelay[timeIndex][1]=AvgDelay[timeIndex][1]+resultSet.getDouble("Delay");
                    AvgDelay[timeIndex][2]=AvgDelay[timeIndex][2]+resultSet.getDouble("Volume")/12;
                }
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        XYSeries xySeries=new XYSeries("Delay");
        for(int i=0;i<NumInterval;i++) {
            xySeries.add(AvgDelay[i][0], AvgDelay[i][1]/AvgDelay[i][2]);
        }
        return xySeries;
    }

    public static XYSeries getAverageDetectorSpeed(List<IntersectionProperty> intersectionPropertyList,Statement ps,int Date, int Interval){
        // Get average detector speed
        int NumInterval=24*3600/Interval;
        int Year=Date/10000;
        double MaxSpeed=75;// mph
        double [][] AvgSpeed=new double[NumInterval][3];
        for(int i=0;i<NumInterval;i++) {
            AvgSpeed[i][0]=i*Interval/3600.0;
            AvgSpeed[i][1] = 0;
            AvgSpeed[i][2] = 0.000001;
        }
        for(int i=0;i<intersectionPropertyList.size();i++){// Loop for each detector
            int DetectorID=intersectionPropertyList.get(i).IntersectionID*100+intersectionPropertyList.get(i).SensorID;
            int NumOfLanes=intersectionPropertyList.get(i).NumOfLanes;
            // Get the data
            String sql="Select Time, Speed from detector_data_raw_"+Year+" where Date="+Date+" and DetectorID="+DetectorID+";";
            try{
                ResultSet resultSet=ps.executeQuery(sql);
                while (resultSet.next()) {
                    int tmpTime=resultSet.getInt("Time");
                    int timeIndex=tmpTime/Interval;
                    // Add up the flow counts: lane-based flow * number of lanes
                    AvgSpeed[timeIndex][1]=AvgSpeed[timeIndex][1]+resultSet.getDouble("Speed")*NumOfLanes;
                    AvgSpeed[timeIndex][2]=AvgSpeed[timeIndex][2]+NumOfLanes;
                }
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        XYSeries xySeries=new XYSeries("Speed");
        for(int i=0;i<NumInterval;i++) {
            xySeries.add(AvgSpeed[i][0], Math.min(MaxSpeed,AvgSpeed[i][1]/AvgSpeed[i][2]*0.681818));
        }
        return xySeries;
    }

    public static XYSeries getAverageDetectorOccupancy(List<IntersectionProperty> intersectionPropertyList,Statement ps,int Date, int Interval){
        // Get average detector occupancy
        int NumInterval=24*3600/Interval;
        int Year=Date/10000;
        double MaxOccupancy=100;
        double [][] AvgOccupancy=new double[NumInterval][3];
        for(int i=0;i<NumInterval;i++) {
            AvgOccupancy[i][0]=i*Interval/3600.0;
            AvgOccupancy[i][1] = 0;
            AvgOccupancy[i][2] = 0.000001;
        }
        for(int i=0;i<intersectionPropertyList.size();i++){// Loop for each detector
            int DetectorID=intersectionPropertyList.get(i).IntersectionID*100+intersectionPropertyList.get(i).SensorID;
            int NumOfLanes=intersectionPropertyList.get(i).NumOfLanes;
            // Get the data
            String sql="Select Time, occupancy from detector_data_raw_"+Year+" where Date="+Date+" and DetectorID="+DetectorID+";";
            try{
                ResultSet resultSet=ps.executeQuery(sql);
                while (resultSet.next()) {
                    int tmpTime=resultSet.getInt("Time");
                    int timeIndex=tmpTime/Interval;
                    // Add up the flow counts: lane-based flow * number of lanes
                    AvgOccupancy[timeIndex][1]=AvgOccupancy[timeIndex][1]+resultSet.getDouble("Occupancy")*NumOfLanes;
                    AvgOccupancy[timeIndex][2]=AvgOccupancy[timeIndex][2]+NumOfLanes;
                }
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        XYSeries xySeries=new XYSeries("Occupancy");
        for(int i=0;i<NumInterval;i++) {
            xySeries.add(AvgOccupancy[i][0], Math.min(MaxOccupancy,AvgOccupancy[i][1]/AvgOccupancy[i][2]/3600*100));
        }
        return xySeries;
    }

    public static XYSeries getTotalDetectorFlowCount(List<IntersectionProperty> intersectionPropertyList,Statement ps,int Date, int Interval){
        // Get total detector flow count
        int NumInterval=24*3600/Interval;
        int Year=Date/10000;
        double [][] TotalFlowCount=new double[NumInterval][2];
        for(int i=0;i<NumInterval;i++) {
            TotalFlowCount[i][0]=i*Interval/3600.0;
            TotalFlowCount[i][1] = 0;
        }
        for(int i=0;i<intersectionPropertyList.size();i++){// Loop for each detector
            int DetectorID=intersectionPropertyList.get(i).IntersectionID*100+intersectionPropertyList.get(i).SensorID;
            int NumOfLanes=intersectionPropertyList.get(i).NumOfLanes;
            // Get the data
            String sql="Select Time, Volume from detector_data_raw_"+Year+" where Date="+Date+" and DetectorID="+DetectorID+";";
            try{
                ResultSet resultSet=ps.executeQuery(sql);
                while (resultSet.next()) {
                    int tmpTime=resultSet.getInt("Time");
                    int timeIndex=tmpTime/Interval;
                    // Add up the flow counts: lane-based flow * number of lanes
                    TotalFlowCount[timeIndex][1]=TotalFlowCount[timeIndex][1]+resultSet.getDouble("Volume")*NumOfLanes;
                }
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        XYSeries xySeries=new XYSeries("Flow");
        for(int i=0;i<NumInterval;i++) {
            xySeries.add(TotalFlowCount[i][0], TotalFlowCount[i][1]);
        }
        return xySeries;
    }

    public static double getPlannedGreenByPercentile(XYSeries xySeries,double Percentile){
        // This function is used to get the planned green times by percentile
        List<Double> PlannedGreenList=new ArrayList<Double>();
        for(int i=0;i<xySeries.getItemCount();i++){
            PlannedGreenList.add(xySeries.getY(i).doubleValue());
        }
        Collections.sort(PlannedGreenList);

        double PlannedGreen=PlannedGreenList.get((int) (PlannedGreenList.size()*Percentile));
        return PlannedGreen;
    }

    public static int[] getColumnFromMatrix(int [][] Input, int Column){
        // This function is used to get a row from a matrix
        int [] Output=new int [Input.length];
        for(int i=0;i<Input.length;i++){
            Output[i]=Input[i][Column];
        }
        return Output;
    }

    public static int[] convertListToArrayInteger(List<Integer> Input){
        // This function is to convert List to Matrix
        int [] Output=new int [Input.size()];
        for(int i=0;i<Input.size();i++){
            Output[i]=Input.get(i);
        }
        return Output;
    }

    public static int[][] convertListToMatrixInteger(List<List<Integer>> Input){
        // This function is to convert List to Matrix
        int [][] Output=new int [Input.size()][Input.get(0).size()];
        for(int i=0;i<Input.size();i++){
            for(int j=0;j<Input.get(0).size();j++){
                Output[i][j]=Input.get(i).get(j);
            }
        }
        return Output;
    }

    public static double [] getYMeasureByTimingStatusByBin(int[] TimeInSecond, int [] YMeasure, List<String> TimingStatusList,
                                                               String TimingStatus, int BinSizeInSecond){
        // This function is used to get y measure by timing status by bin

        List<Double> ySeries=new ArrayList<Double>(); // Get a new y series
        int NumberOfBin= 24*3600/BinSizeInSecond;
        for(int i=0;i<NumberOfBin;i++){// Loop for each bin
            double CurTime=((i+0.5)*BinSizeInSecond)/3600.0;
            for(int j=0;j<TimeInSecond.length;j++){// Loop for each observation
                if(TimingStatusList.get(j).equals(TimingStatus)) { // Find the right timing status
                    if (TimeInSecond[j] >= BinSizeInSecond * i && TimeInSecond[j] < (i + 1) * BinSizeInSecond) {
                        for(int k=0;k<YMeasure[j];k++){
                            ySeries.add(CurTime);
                        }
                    }
                }
            }
        }
        double [] ySeriesDouble=new double[ySeries.size()];
        for (int i=0;i<ySeries.size();i++){
            ySeriesDouble[i]=ySeries.get(i);
        }
        return ySeriesDouble;
    }

    public static XYSeries getXTimeYMeasureByTimingStatus(int[] TimeInSecond, int [] YMeasure, List<String> TimingStatusList, String TimingStatus){
        // This function is used to get x-y measures by timing status

        XYSeries xySeries=new XYSeries(TimingStatus); // Get a new xy series
        double TimeInHour;
        for(int i=0;i<TimeInSecond.length;i++){ // Loop for each row
            if(TimingStatusList.get(i).equals(TimingStatus)) { // Find the right timing status
                // Add a new point
                TimeInHour = TimeInSecond[i] / 3600.0;
                xySeries.add(TimeInHour, YMeasure[i]);
            }
        }
        return xySeries;
    }


}
