package PlotFunctions;

/**
 * Created by Qijian-Gan on 1/29/2018.
 */
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.awt.*;

public class writeToHTML {

    // Database
    public static String hostPasadena="jdbc:mysql://localhost:3306/pasadena_data";  // For Pasadena data
    // Users
    public static String userName="root";
    public static String password="!Ganqijian2017";
    // Variables
    public static Connection conPasadena;

    public static class IntersectionFormat{
        public IntersectionFormat(int _IntersectionID,String _IntersectionName, int _NumDetectors, double _Latitude, double _Longitude,int _GoodDetectors){
            this.IntersectionID=_IntersectionID;
            this.IntersectionName=_IntersectionName;
            this.NumDetectors=_NumDetectors;
            this.Latitude=_Latitude;
            this.Longitude=_Longitude;
            this.GoodDetectors=_GoodDetectors;
        }
        public int IntersectionID;
        public String IntersectionName;
        public int NumDetectors;
        public int GoodDetectors;
        public double Latitude;
        public double Longitude;
    }

    public static void main(final String[] args) {

        try {
            int startDate=20171203;
            int endDate=20171223;
            String OutputFolder="L:\\arterial_detector_analysis_files\\html";

            conPasadena = DriverManager.getConnection(hostPasadena, userName, password);
            System.out.println("Succefully connect to the database!");
            Statement ps=conPasadena.createStatement();

            List<IntersectionFormat> intersectionFormats=readIntersectionInv(ps,startDate);
            intersectionFormats=updateGoodDetectors(ps,startDate,endDate,intersectionFormats);
            String DateStr="From: "+ startDate+ " To: "+ endDate;
            writeDataToHTML(intersectionFormats,DateStr,OutputFolder);

            Runtime rTime = Runtime.getRuntime();
            String url = OutputFolder+"/IntersectionHealth.html";
            String browser = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe ";
            Process pc = rTime.exec(browser + url);
            pc.waitFor();

        }
        catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<IntersectionFormat> updateGoodDetectors(Statement ps,int startDate,int endDate
            ,List<IntersectionFormat> intersectionFormatList){

        int NumOfDays=endDate-startDate+1;
        try {
            for (int i = 0; i < intersectionFormatList.size(); i++) {
                int NumDetectors = intersectionFormatList.get(i).NumDetectors;
                int IntersectionID = intersectionFormatList.get(i).IntersectionID;
                String sql = "SELECT count(DetectorID) FROM detector_health where floor(DetectorID/100)=" + IntersectionID +
                        " and (Year*10000+Month*100+Day>=" + startDate + " and Year*10000+Month*100+Day<=" + endDate + ") and " +
                        "Health=1;";
                ResultSet result = ps.executeQuery(sql);
                result.next();
                int NumGoodDetectors=result.getInt("count(DetectorID)");
                intersectionFormatList.get(i).GoodDetectors=NumGoodDetectors/NumOfDays;
                //System.out.println("IntID="+IntersectionID+ " Det="+NumDetectors +" and GoodDet=" +NumGoodDetectors/NumOfDays);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return intersectionFormatList;
    }

    public static List<IntersectionFormat> readIntersectionInv(Statement ps,int Date){

        List<IntersectionFormat> intersectionFormatList=new ArrayList<IntersectionFormat>();

        try {
            String sql = "SELECT distinct detector_inventory.IntersectionID, detector_inventory.MainStreet, detector_inventory.CrossStreet, " +
                    "intersection_latitude_longitude.Latitude, intersection_latitude_longitude.Longitude FROM detector_inventory inner join " +
                    "intersection_latitude_longitude on detector_inventory.IntersectionID=intersection_latitude_longitude.IntersectionID " +
                    "where detector_inventory.LastUpdateDate=" + Date + ";";
            ResultSet result = ps.executeQuery(sql);
            while (result.next()) {
                int IntersectionID=result.getInt("IntersectionID");
                String IntersectionName=result.getString("MainStreet")+"@"+result.getString("CrossStreet");
                double Latitude=result.getDouble("Latitude");
                double Longitude=result.getDouble("Longitude");
                IntersectionFormat intersectionFormat=new IntersectionFormat(IntersectionID,IntersectionName,0,Latitude,Longitude,0);
                intersectionFormatList.add(intersectionFormat);
            }

            for(int i=0;i<intersectionFormatList.size();i++){
                int IntersectionID=intersectionFormatList.get(i).IntersectionID;
                // Update the number of detectors inside each intersection
                String sql1="SELECT count(distinct DetectorID) FROM detector_inventory where IntersectionID="+IntersectionID+
                        " and LastUpdateDate=" + Date + ";";
                ResultSet result1=ps.executeQuery(sql1);
                result1.next();
                int NumDetectors=result1.getInt("count(distinct DetectorID)");
                intersectionFormatList.get(i).NumDetectors=NumDetectors;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return intersectionFormatList;
    }

    public static void writeDataToHTML(List<IntersectionFormat>intersectionFormats,String DateStr,String OutputFolder){

        File fileDir = new File(OutputFolder);
        File htmlFile = new File(fileDir,"IntersectionHealth.html");
        htmlFile.delete();
        try {
            FileWriter fw;
            fw = new FileWriter(htmlFile.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("<html>\n" +
                    "<head>\n" +
                    "    <script type=\"text/javascript\" src=\"https://www.gstatic.com/charts/loader.js\"></script>\n" +
                    "    <script language=\"javascript\" type=\"text/javascript\" src=\"jquery.js\"></script>\n" +
                    "</head>\n" +
                    "<body>\n" +
                    "<h2> <center>Intersection Sensor Status in Pasadena </center> </h2>\n");

            bw.write("<h3> <center>"+DateStr+"</center> </h3>\n");
            bw.write("<h4><center><IMG SRC=\"http://icons.iconarchive.com/icons/icons-land/vista-map-markers/24/Map-Marker-Marker-Outside-Chartreuse-icon.png\" ALT=\"Good\" WIDTH=24 HEIGHT=24> Working\n" +
                    "    <IMG SRC=\"http://icons.iconarchive.com/icons/icons-land/vista-map-markers/24/Map-Marker-Marker-Outside-Azure-icon.png\" ALT=\"Good\" WIDTH=24 HEIGHT=24> Partially Working\n" +
                    "    <IMG SRC=\"http://icons.iconarchive.com/icons/icons-land/vista-map-markers/24/Map-Marker-Marker-Outside-Pink-icon.png\" ALT=\"Good\" WIDTH=24 HEIGHT=24> Not Working\n" +
                    "</center></h4>");
            bw.write(
                    "<script id=\"source\" language=\"javascript\" type=\"text/javascript\">\n" +
                    "   google.charts.load(\"current\", {\n" +
                    "      \"packages\":[\"map\"],\n" +
                    "      \"mapsApiKey\": \"AIzaSyD-9tSrke72PouQMnMX-a7eZSW0jkFMBWY\"\n" +
                    "   });\n" +
                    "   google.charts.setOnLoadCallback(drawChart);\n" +
                    "   function drawChart() {\n"
                    );

            // Create the data strings
            String strData="     var arrayGoodOrBad=[\n" +
                    "       ['Lat', 'Long', 'Name','Color'],\n";
            for(int i=0;i<intersectionFormats.size();i++){
                String IntersectionName=intersectionFormats.get(i).IntersectionName;
                double Latitude=intersectionFormats.get(i).Latitude;
                double Longitude=intersectionFormats.get(i).Longitude;
                String Status;
                if(intersectionFormats.get(i).GoodDetectors==0){
                    Status="pink";
                }else if(intersectionFormats.get(i).GoodDetectors==intersectionFormats.get(i).NumDetectors){
                    Status="green";
                }else{
                    Status="blue";
                }

                strData=strData+ "["+Latitude+","+Longitude+",\'"+IntersectionName+"\',\'"+Status+"\'],\n";
            }
            strData=strData+"     ]\n";
            bw.write(strData);
            bw.write(
                    "     var data = google.visualization.arrayToDataTable(arrayGoodOrBad);\n" +
                            "     var url = 'http://icons.iconarchive.com/icons/icons-land/vista-map-markers/48/';\n" +
                            "     var options = {\n" +
                            "       showTooltip: true,\n" +
                            "       showInfoWindow: true,\n" +
                            "       useMapTypeControl: true,\n" +
                            "       icons: {\n" +
                            "         green: {\n" +
                            "           normal:   url + 'Map-Marker-Marker-Outside-Chartreuse-icon.png',\n" +
                            "           selected: url + 'Map-Marker-Marker-Outside-Chartreuse-icon.png'\n" +
                            "         },\n" +
                            "         pink: {\n" +
                            "           normal:   url + 'Map-Marker-Marker-Outside-Pink-icon.png',\n" +
                            "           selected: url + 'Map-Marker-Marker-Outside-Pink-icon.png',\n" +
                            "         },\n" +
                            "         blue: {\n" +
                            "           normal:   url + 'Map-Marker-Marker-Outside-Azure-icon.png',\n" +
                            "           selected: url + 'Map-Marker-Marker-Outside-Azure-icon.png'\n" +
                            "         }\n" +
                            "       }\n" +
                            "     };\n" +
                            "     var map = new google.visualization.Map(document.getElementById('map'));\n" +
                            "     map.draw(data, options);\n" +
                            "   };\n" +
                            "    </script>\n" +
                            "<style>\n" +
                            "      #map {\n" +
                            "        height: 85%;\n" +
                            "      }\n" +
                            "      html, body {\n" +
                            "        height: 100%;\n" +
                            "        margin: 0;\n" +
                            "        padding: 0;\n" +
                            "      }\n" +
                            "    </style>\n" +
                            "<div id=\"map\"></div>\n" +
                            "</body>\n" +
                            "</html>");

            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
