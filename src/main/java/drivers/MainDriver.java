package drivers;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import drivers.Task1Driver;
import drivers.Task2Driver;
import drivers.Task3Driver;
import drivers.Task4Driver;

public class MainDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <input path> <output directory>");
            System.exit(-1);
        }

        //Run Tasks
        runTasks(args);

        System.out.println("\n\nRan all jobs...");

        //Get Result Data
        getData(args[1]);


    }

    private static void runTasks(String[] args) {
        try {
            Task1Driver.run(args[0], args[1] + "/task1");
            
            Task2Driver.run(args[0], args[1] + "/task2");
            
            Task3Driver.run(args[0], args[1] + "/task3");
            
            Task4Driver.run(args[0], args[1] + "/task4");
            
            Task5Driver.run(args[0], args[1] + "/task5");

            Task6Driver.run(args[0], args[1] + "/task6");
        }
        catch(Exception ex) {
            System.out.println("There was an error processing tasks.");
        }
    }

    private static void getData(String path) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://136.145.215.153:50070/");
            FileSystem hdfsFileSystem = FileSystem.get(conf);
            
            Path local = new Path("download/");
            Path hdfs = new Path(path + "/task1/part-r-00000");
            hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);
            
            //Massage Data for JS

        }
        catch(IOException ex) {
            System.out.println("There was an error accessing HDFS:\n" + ex.getMessage());
        }
        
    }
}
