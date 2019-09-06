package networkConnection;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.text.*;
import java.lang.Object.*;

/**
 * Created by Qijian-Gan on 9/25/2017.
 */
public class networkConnection {

    public static void mainTestVPNConnection(String ServerName, String User, String Password){
        // Start the connection
        networkConnection.connectToVPN(ServerName,User,Password);
        // Close the connection
        networkConnection.disconnectFromVPN(ServerName);
    }

    public static void mainTestMapRemoteServer(String ServerName,String IP, String FolderLocation,String User, String PW){
        // Start the connection
        deleteMappedFolderFromRemoteServer("x");
        networkConnection.connectToVPN(ServerName,User,PW);
        mapDetectorFolderFromRemoteServer("x",IP, FolderLocation,User, PW);
        File fileDir = new File("x:\\");
        File[] listOfFiles = fileDir.listFiles();
        for(int i=0;i<listOfFiles.length;i++){
            System.out.println(listOfFiles[i].getName());
        }
        deleteMappedFolderFromRemoteServer("x");
        // Close the connection
        networkConnection.disconnectFromVPN(ServerName);
    }

    public static boolean connectToVPN(String ServerName, String UserName, String Password){
        // This function is used to connect to the VPN

        String command="rasdial \""+ ServerName+ "\" \"" + UserName + "\" \"" + Password+ "\"";

        try {
            Process p=Runtime.getRuntime().exec(command);
            System.out.println("Succeed to start the VPN connection!");
            return true;
        } catch (IOException e) {
            System.out.println("Fail to start the VPN connection!");
            return false;
        }
    }

    public static boolean disconnectFromVPN(String ServerName){
        // This function is used to disconnect from the VPN

        String command="rasdial \""+ ServerName+ "\""+" /DISCONNECT";

        try {
            Process p=Runtime.getRuntime().exec(command);
            System.out.println("Succeed to stop the VPN connection!");
            return true;
        } catch (IOException e) {
            System.out.println("Fail to stop the VPN connection!");
            return false;
        }
    }

    public static boolean mapDetectorFolderFromRemoteServer(String Disk,String IP, String FolderLocation,String User, String PW){
        // This function is used to map detector folder from the remoted server
        String command="net use "+Disk+": \\\\"+IP+"\\"+FolderLocation+"  /user:"+User+" "+PW;
        try {
            Process p=Runtime.getRuntime().exec(command);
            System.out.println("Succeed to map the remoted folder "+FolderLocation+" to "+Disk);
            return true;
        } catch (IOException e) {
            System.out.println("Fail to map the remoted folder!");
            return false;
        }
    }

    public static boolean deleteMappedFolderFromRemoteServer(String Disk){
        // This function is used to delete the mapped detector folder from the remoted server
        String command="net use "+Disk+": /delete";
        try {
            Process p=Runtime.getRuntime().exec(command);
            System.out.println("Succeed to delete the mapped folder!");
            return true;
        } catch (IOException e) {
            System.out.println("Fail to delete the mapped folder!");
            return false;
        }
    }
}
