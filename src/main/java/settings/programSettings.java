package settings;

/**
 * Created by Qijian-Gan on 10/5/2017.
 */
public class programSettings {

    // Users
    public  static String userName=null;
    public  static String password=null;
    //public static String userName="PATH";
    //public static String password="!PATH2017Arterial";

    // Configuration files
    public  static String configDirArcadia=null;
    public  static String configNameArcadia=null;
    public  static String configDirLACO=null;
    public  static String configNameLACO=null;

    // For reading IEN Raw Data
    public  static String rawIENDataDir=null; // Read data from ...
    public  static String rawIENDataNewDir=null; // Remove files to...

    // For Pasadena Data
    public  static String rawPasadenaDataDir=null; // Read data from ...
    public  static String rawPasadenaDataNewDir=null; // Remove files to...

    // For reading raw TCS data to the database
    public  static String rawTCSDataDir=null;
    public  static String rawTCSDataNewDir=null;

    // For performing detector health analysis and data filtering
    public  static String fromDate=null;
    public  static String toDate=null;
    public  static String dataSourceHealth=null; //TCS server or IEN
    public  static String organizationHealth=null; //Arcadia or LACO
    public  static int defaultInterval=0; //Default interval

    // Threshold for TCS server
    public  static double missingRateThreshold_TCSServer=0;
    public  static double maxZeroValueThreshold_TCSServer=0;
    public  static double highValueRateThreshold_TCSServer=0;
    public  static double highFlowValue_TCSServer=0;
    public  static double inconsisRateWithoutSpeedThreshold_TCSServer=0;

    // Threshold for the IEN
    public  static double missingRateThreshold_IEN=0;
    public  static double maxZeroValueThreshold_IEN=0;
    public  static double highValueRateThreshold_IEN=0;
    public  static double highFlowValue_IEN=0;
    public  static double inconsisRateWithoutSpeedThreshold_IEN=0;

    // Setting for smoothing data
    public  static String methodSmoothing=null;
    public  static int spanSmoothing=0;

    // Setting for imputation
    public  static int spanImputation=0;
    public  static int useMedianOrNotImputation=-1;

    // For extracting detector health analysis (Require configuration file: configDir & configName)
    public  static String healthOutputFolder=null; //Output folder
    public  static String dataSource=null; //Data source
    public  static String organization=null; //Organization
    public  static String startDateString=null;// Starting date
    public  static String endDateString=null; // Ending date

    // For loading existing detector health file to database (From matlab to Java)
    public  static String configDirHealth=null;

    // For data aggragation (Manually)
    public  static String method=null;
    public  static String fromDateString=null;
    public  static String toDateString=null;
    public  static int interval=0; // In seconds
    public  static String organizationAggregation=null;

    // For reading signal phasing data to the database
    // Phase times
    public  static String phaseTimeDataDir=null;
    public  static String phaseTimeDataNewDir=null;
    // Timing plans
    public  static String timingPlansDataDir=null;
    public  static String timingPlansDataNewDir=null;

}
