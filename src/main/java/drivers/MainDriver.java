package drivers;

import java.io.File;
import java.io.PrintWriter;
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
            System.out.println("Connecting to HDFS...");
            Configuration conf = new Configuration();
            FileSystem hdfsFileSystem = FileSystem.get(conf);
            Path local = new Path("downloads/part-r-00000");

            System.out.println("Getting Task 1 result...");
            Path hdfs = new Path(path + "/task1/part-r-00000");
            hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);

            System.out.println("Massaging Task 1 result...");
            File task1Data = new File("downloads/part-r-00000");
            Scanner task1File = new Scanner(task1Data);
            String outputText = "var task1Data = [";
            while(task1File.hasNext()) {
                outputText += "{text: '" + task1File.next() + "', size: " + task1File.next() + "},";
            }
            outputText = outputText.substring(0, outputText.length()-1);
            outputText += "];";
            task1File.close();
            File outputFile = new File("results/task1.js");
            PrintWriter output = new PrintWriter(outputFile);
            output.write(outputText);
            output.close();
            
            System.out.println("Massaging Task 2 result...");
            //local = new Path("downloads/part-r-000002");
            hdfs = new Path(path + "/task2/part-r-00000");
            hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);
            File task2Data = new File("downloads/part-r-00000");
            Scanner task2File = new Scanner(task2Data);
            outputText = "var task2Data = [";
            while(task2File.hasNext()) {
                String text = task2File.next();
                String size = task2File.next();
                outputText += "{text: '" + text + "', size: " + size + "},";
                System.out.println(text + " " + size);
            }
            outputText = outputText.substring(0, outputText.length()-1);
            outputText += "];";
            outputFile = new File("results/task2.js");
            output = new PrintWriter(outputFile);
            output.write(outputText);
            output.close();
            task2File.close();

            System.out.println("Massaging Task 3 result...");
            //local = new Path("downloads/part-r-000003");
            hdfs = new Path(path + "/task3/part-r-00000");
            hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);
            File task3Data = new File("downloads/part-r-00000");
            Scanner task3File = new Scanner(task3Data);
            outputText = "var task3Data = [";
            while(task3File.hasNext()) {
                outputText += "{username: '" + task3File.next() + "', count: " + task3File.next() + "},";
            }
            outputText = outputText.substring(0, outputText.length()-1);
            outputText += "];";
            outputFile = new File("results/task3.js");
            output = new PrintWriter(outputFile);
            output.write(outputText);
            output.close();
            task3File.close();

        }
        catch(IOException ex) {
            System.out.println("There was an error accessing HDFS:\n" + ex.getMessage());
        }
        
    }
}
