package analysis;

import java.sql.Connection;
import java.util.Arrays;
import Utility.util;
import main.MainFunction;
import saveData.saveDataToDatabase;


/**
 * Created by Qijian-Gan on 9/23/2017.
 */
public class dataFiltering {
// This is the class for data filtering

    public static void mainDataFiltering(Connection con, double [][] dataInput, int detectorID, int year, int month, int day, int interval){

        // Fill in missing values
        ImputationSetting imputationSetting= new ImputationSetting(MainFunction.cBlock.spanImputation,
                MainFunction.cBlock.useMedianOrNotImputation);

        double [][] dataFillInMissingValue=dataFiltering.fillInMissingValues(dataInput,interval,imputationSetting);

        // Smooth the data
        SmoothSetting smoothSetting= new SmoothSetting(MainFunction.cBlock.methodSmoothing,MainFunction.cBlock.spanSmoothing);
        double [][] dataSmooth=dataFiltering.smoothingData(dataFillInMissingValue,smoothSetting);

        // Insert the processed data to database
        saveDataToDatabase.insertProcessedTCSDataToDataBase(con, dataSmooth, detectorID, year, month,day);
    }

    public static class ImputationSetting{
        // This is the settings for imputation

        public ImputationSetting(int _span, int _useMedianOrNot){

            this.span=_span; // Set the span for imputation
            this.useMedianOrNot=_useMedianOrNot; // Whether to use the median value or not
        }
        int span;
        int useMedianOrNot;
    }

    public static class SmoothSetting{
        // This is the function for smoothing

        public SmoothSetting(String _method, int _span){

            this.span=_span; // Set the span for imputation: moving average
            this.method=_method; // Set the method
        }
        int span;
        String method;
    }

    public static double [][] smoothingData(double [][] dataInput, SmoothSetting smoothSetting){
        // This function is used to smooth the input data

        // dataInput: time, flow, occupancy, and speed

        // Create the same size of the matrix
        double [][] dataOutput= new double[dataInput.length][dataInput[0].length];

        // Check the smoothing method
        if(smoothSetting.method.equals("MovingAverage")){
            // Method: moving average
            if(smoothSetting.span>=dataInput.length){
                System.out.println("Span for moving average is longer than the array length!");
            }else{
                for(int i=0;i<dataInput.length;i++){ // Loop for all rows
                    dataOutput[i][0]=dataInput[i][0]; //Get the time stamps
                    if(i<smoothSetting.span){ // Less than the span
                        dataOutput[i][1]=dataInput[i][1];
                        dataOutput[i][2]=dataInput[i][2];
                        dataOutput[i][3]=dataInput[i][3];
                    }else{ // If longer than the span
                        // Select the data
                        double [] tmpFlow=util.fromMatrixToArrayByColumn(dataInput,1,i-smoothSetting.span,i-1);
                        double [] tmpOcc=util.fromMatrixToArrayByColumn(dataInput,2,i-smoothSetting.span,i-1);
                        double [] tmpSpeed=util.fromMatrixToArrayByColumn(dataInput,3,i-smoothSetting.span,i-1);

                        // Calculate the mean
                        dataOutput[i][1]=util.calculateMean(tmpFlow);
                        dataOutput[i][2]=util.calculateMean(tmpOcc);
                        dataOutput[i][3]=util.calculateMean(tmpSpeed);
                    }
                }
            }
        }else{
            System.out.println("Unknown smoothing method!");
        }

        return dataOutput;
    }

    public static double[][] fillInMissingValues(double [][] dataInput, int interval, ImputationSetting imputationSetting){
        // This function is used to fillInMissingValues

        // dataInput: Time,Flow,Occupancy,Speed (four columns);
        // Important: all values are SORTED in time.

        int numOfIntervals=24*3600/interval; //Set the number of intervals

        double [][] dataOutput= new double[numOfIntervals][dataInput[0].length];

        int curIndex=0; // Set the current index to be zero
        for(int i=0; i<numOfIntervals; i++){ // Loop for the number of intervals

            double time=i*interval; // Get the current time stamp

            //Search for the right time index
            double sumFlow=0;
            double sumOcc=0;
            double sumSpeed=0;
            double numOfSample=0;
            int j;
            for (j=curIndex;j<dataInput.length;j++){
                if(dataInput[j][0]>=time && dataInput[j][0]<time+interval){
                    // It is possible that the reported time from the raw data is within [time, time+interval]
                    // It is also possible to have multiple data points for a given interval, therefore we take the averages
                    sumFlow=sumFlow+dataInput[j][1];
                    sumOcc=sumOcc+dataInput[j][2];
                    sumSpeed=sumSpeed+dataInput[j][3];
                    numOfSample=numOfSample+1;
                }
                if(dataInput[j][0]>=time+interval)
                { // If it is out of the range of the searching interval
                    break;
                }
            }
            // Update the current index
            // If j==dataInput.length, it reaches the end of the input data, and the output data will use the smoothed
            // values of the previous steps. If no data samples are found, j would not change
            curIndex=j;

            // Check whether the corresponding data points are found or not
            dataOutput[i][0]=time;
            if(numOfSample>=1){
                // If have samples in the corresponding time interval, take the mean
                dataOutput[i][1]=sumFlow/numOfSample;
                dataOutput[i][2]=sumOcc/numOfSample;
                dataOutput[i][3]=sumSpeed/numOfSample;
            }else{
                // If no samples
                if(i==0){
                    // If it is the first data point, set it to be the first value of dataInput
                    dataOutput[i][1]=dataInput[0][1];
                    dataOutput[i][2]=dataInput[0][2];
                    dataOutput[i][3]=dataInput[0][3];
                }else{
                    double [] tmpFlow;
                    double [] tmpOcc;
                    double [] tmpSpeed;
                    if(i<imputationSetting.span){
                        // If it is less than the span
                        tmpFlow=util.fromMatrixToArrayByColumn(dataOutput,1,0,i-1);
                        tmpOcc=util.fromMatrixToArrayByColumn(dataOutput,2,0,i-1);
                        tmpSpeed=util.fromMatrixToArrayByColumn(dataOutput,3,0,i-1);
                    }else{
                        // If it is greater than the span
                        tmpFlow=util.fromMatrixToArrayByColumn(dataOutput,1,i-imputationSetting.span,i-1);
                        tmpOcc=util.fromMatrixToArrayByColumn(dataOutput,2,i-imputationSetting.span,i-1);
                        tmpSpeed=util.fromMatrixToArrayByColumn(dataOutput,3,i-imputationSetting.span,i-1);
                    }

                    if(imputationSetting.useMedianOrNot==1) {
                        // Use median
                        dataOutput[i][1] = util.calculateMedian(tmpFlow);
                        dataOutput[i][2] = util.calculateMedian(tmpOcc);
                        dataOutput[i][3] = util.calculateMedian(tmpSpeed);
                    }else
                    {// Use mean
                        dataOutput[i][1] = util.calculateMean(tmpFlow);
                        dataOutput[i][2] = util.calculateMean(tmpOcc);
                        dataOutput[i][3] = util.calculateMean(tmpSpeed);
                    }
                }
            }
        }
        return dataOutput;
    }
}

