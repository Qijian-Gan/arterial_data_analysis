package main;

import ATSPMs.calculateATSMPs;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import settings.programSettings;

/**
 * Created by Qijian-Gan on 11/21/2017.
 */
public class MainATSPMS {
    // ***********Global settings************
    // Database
    public static String host="jdbc:mysql://localhost:3306/arcadia_tcs_server_data"; // For TCS server
    public static String hostPasadena="jdbc:mysql://localhost:3306/pasadena_data";  // For Pasadena data
    public static String hostIENArcadia="jdbc:mysql://localhost:3306/arcadia_ien_server_data"; // For IEN-Arcadia
    public static String hostIENLACO="jdbc:mysql://localhost:3306/laco_ien_server_data";  // For IEN-LACO
    // Users
    public static String userName="root";
    public static String password="!Ganqijian2017";
    // Variables
    public static Connection conTCSServer;

    public static void main(final String[] args) {

        try {
            conTCSServer = DriverManager.getConnection(host, userName, password);
            System.out.println("Succefully connect to the database!");

            int Year=2017;
            int Date=20171110;

            // Get the list of intersections
            Statement ps=conTCSServer.createStatement();
            String sql="SELECT distinct IntersectionID FROM phase_time_"+Year+" where EndDate="+Date+";";
            ResultSet result = ps.executeQuery(sql);
            List<Integer> IntersectionIDList=new ArrayList<Integer>();
            while (result.next()) {
                IntersectionIDList.add(result.getInt("IntersectionID"));
            }

            // Loop for each intersection
            for(int i=46;i<47;i++){
            //for(int i=0;i<IntersectionIDList.size();i++){

                // ****************Get the detector data*****************
                String invIntID="Select * from detector_inventory where IntersectionID="+IntersectionIDList.get(i)+";";
                ResultSet resultInvIntID = ps.executeQuery(invIntID);
                List<calculateATSMPs.IntersectionProperty> IntersectionPropertyList=
                        calculateATSMPs.getIntersectionProperty(resultInvIntID);
                int Interval=300;
                calculateATSMPs.drawApproachFlowAndOccupancy(IntersectionPropertyList, ps,Date,Interval, "Flow");
                calculateATSMPs.drawApproachFlowAndOccupancy(IntersectionPropertyList, ps,Date,Interval, "Occupancy");
                calculateATSMPs.drawApproachFlowAndOccupancy(IntersectionPropertyList, ps,Date,Interval, "Speed");
                calculateATSMPs.drawApproachFlowAndOccupancy(IntersectionPropertyList, ps,Date,Interval, "Time&Flow&Occupancy");
                calculateATSMPs.drawApproachFlowAndOccupancy(IntersectionPropertyList, ps,Date,Interval, "Flow&Occupancy");
                calculateATSMPs.drawApproachFlowAndOccupancy(IntersectionPropertyList, ps,Date,Interval, "Delay");
                calculateATSMPs.drawTurningMovementCounts(IntersectionPropertyList, ps, Date,Interval);



                // ****************Get the signal information******************
                String sqlIntID="SELECT * FROM phase_time_"+Year+" where EndDate="+Date+
                        " and IntersectionID="+IntersectionIDList.get(i)+";";
                ResultSet resultIntID = ps.executeQuery(sqlIntID);
                calculateATSMPs.PhaseTimeList phaseTimeList=calculateATSMPs.getSQLData(resultIntID);
                // Draw split monitor
                calculateATSMPs.drawSplitMonitor(phaseTimeList);
                // Draw Pedestrian Calls
                calculateATSMPs.drawPedestrianCalls(phaseTimeList,"ByFixedInterval",900);
/**/

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
