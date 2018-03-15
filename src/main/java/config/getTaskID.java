package config;

import java.util.Scanner;

/**
 * Created by Qijian-Gan on 10/6/2017.
 */
public class getTaskID {
    public static int getTaskIDFromScreen(){
        int taskID=0;
        // Selection of type of tasks
        Scanner scanner = new Scanner(System.in);
        System.out.print("Please choose one of the following tasks (Be sure to update the configuration file FIRST!):\n");
        System.out.print("1:  Insert Detector Config To DataBase\n"); // Load configuration files
        System.out.print("2:  Extract Health Results To File\n"); // Extract the health results
        System.out.print("3:  Load Arcadia TCS Server Raw Data To DataBase\n"); // Load TCS raw data
        System.out.print("4:  Load Pasadena Data To DataBase\n"); // Load Pasadena data
        System.out.print("5:  Load IEN Data To DataBase\n"); // Load IEN data
        System.out.print("6:  Aggregation Of The IEN/Pasadena Data\n"); // Data aggregation
        System.out.print("7:  Detector Health Analysis & Data Filtering And Imputation\n"); // Health analysis and data filtering
        System.out.print("8:  Load Health File(csv) To DataBase\n");
        System.out.print("9:  Load TCS Signal Phasing data To DataBase\n");
        System.out.print("10:  Get real-time data from TCS server!\n");
        System.out.print("11:  Perform automatic detector health analysis!\n");
        System.out.print("12:  Perform automatic aggregation and detector health analysis for LACO & Arcadia from IEN!\n");
        System.out.print("Please enter your selection (number):");
        taskID =Integer.parseInt(scanner.next());
        return taskID;
    }
}
