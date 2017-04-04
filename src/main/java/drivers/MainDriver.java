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
        if (args.length != 3) {
            System.err.println("Usage: <input path> <output directory> <true or false>");
            System.exit(-1);
        }

         //Run Tasks
         runTasks(args);

        System.out.println("\n\nRan all jobs...");

        //Get Result Data
        if(args[3].toLowerCase().equals("true")) {
                getData(args[1]);
                System.out.println("\n\nRan massagers...");
            }


    }

    private static void runTasks(String[] args) {
        try {
            //Task1Driver.run(args[0], args[1] + "/task1");
            
            Task2Driver.run(args[0], args[1] + "/task2");
            
            //Task3Driver.run(args[0], args[1] + "/task3");
            
            //Task4Driver.run(args[0], args[1] + "/task4");
            
            //Task5Driver.run(args[0], args[1] + "/task5");

            //Task6Driver.run(args[0], args[1] + "/task6");
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
            Path hdfs;
            String outputText;
            File outputFile;
            PrintWriter output;

            // System.out.println("Getting Task 1 result...");
            // Path hdfs = new Path(path + "/task1/part-r-00000");
            // hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);

            // System.out.println("Massaging Task 1 result...");
            // File task1Data = new File("downloads/part-r-00000");
            // Scanner task1File = new Scanner(task1Data);
            // String outputText = "var task1Data = [";
            // while(task1File.hasNext()) {
            //     outputText += "{text: '" + task1File.next() + "', size: " + task1File.next() + "},";
            // }
            // outputText = outputText.substring(0, outputText.length()-1);
            // outputText += "];\n";
            // task1File.close();
            // File outputFile = new File("results/task1.js");
            // PrintWriter output = new PrintWriter(outputFile);
            // output.write(outputText);
            // output.close();
            
            System.out.println("Massaging Task 2 result...");
            hdfs = new Path(path + "/task2/part-r-00000");
            hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);
            File task2Data = new File("downloads/part-r-00000");
            Scanner task2File = new Scanner(task2Data);
            outputText = "var task2Data = [";
            task2File.next();
            while(task2File.hasNext()) {
                String text = task2File.next();
                String size = task2File.next();
                try {
                    Integer.parseInt(size);
                }
                catch(Exception ex) {
                    size = task2File.next();
                }
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

            // System.out.println("Massaging Task 3 result...");
            // hdfs = new Path(path + "/task3/part-r-00000");
            // hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);
            // File task3Data = new File("downloads/part-r-00000");
            // Scanner task3File = new Scanner(task3Data);
            // int total = 0;
            // outputText = "var task3Data = [";
            // while(task3File.hasNext()) {
            //      String text = task3File.next();
            //     String size = task3File.next();
            //     try {
            //         Integer.parseInt(size);
            //     }
            //     catch(Exception ex) {
            //         size = task3File.next();
            //     }
            //     outputText += "{username: '" + text + "', count: " + size + "},";
            //     System.out.println(text + " " + size);
            //     total++;
            // }
            // outputText = outputText.substring(0, outputText.length()-1);
            // outputText += "];\n";
            //outputText += "var totalData = " + total + ";";
            // outputFile = new File("results/task3.js");
            // output = new PrintWriter(outputFile);
            // output.write(outputText);
            // output.close();
            // task3File.close();

        //      System.out.println("Massaging Task 4 result...");
        //     hdfs = new Path(path + "/task4/part-r-00000");
        //     hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);
        //     File task4Data = new File("downloads/part-r-00000");
        //     Scanner task4File = new Scanner(task4Data);
        //     outputText = "var task4Data = { name: 'Retweets', children: [";
        //     while(task4File.hasNext()) {
        //         String text = task4File.next();
        //         String[] ids = task4File.next().split(",");

        //         outputText += "{name: 'ID': ''" + text + "', children: [";
        //         for(String id : ids) {
        //             outputText += "{name: '"+ id +"'},";
        //         }
        //         outputText += "]}]};";
        //         System.out.println("Still working on task 4...");
        //     }
        //     outputText = outputText.substring(0, outputText.length()-1);
        //     outputText += "];";
        //     outputFile = new File("results/task4.js");
        //     output = new PrintWriter(outputFile);
        //     output.write(outputText);
        //     output.close();
        //     task4File.close();

        //    System.out.println("Massaging Task 5 result...");
        //     hdfs = new Path(path + "/task5/part-r-00000");
        //     hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);
        //     File task5Data = new File("downloads/part-r-00000");
        //     Scanner task5File = new Scanner(task5Data);
        //     outputText = "var task5Data = { name: 'Replies', children: [";
        //     while(task5File.hasNext()) {
        //         String text = task5File.next();
        //         String[] ids = task5File.next().split(",");

        //         outputText += "{name: 'ID': ''" + text + "', children: [";
        //         for(String id : ids) {
        //             outputText += "{name: '"+ id +"'},";
        //         }
        //         outputText += "]}]};";
        //         System.out.println("Still working on task 5...");
        //     }
        //     outputText = outputText.substring(0, outputText.length()-1);
        //     outputText += "];";
        //     outputFile = new File("results/task5.js");
        //     output = new PrintWriter(outputFile);
        //     output.write(outputText);
        //     output.close();
        //     task5File.close();

        //     System.out.println("Massaging Task 6 result...");
        //     hdfs = new Path(path + "/task6/part-r-00000");
        //     hdfsFileSystem.copyToLocalFile(false, hdfs, local, true);
        //     File task6Data = new File("downloads/part-r-00000");
        //     Scanner task6File = new Scanner(task6Data);
        //     outputText = "var task6Data = { name: 'Tweets', children: [";
        //     while(task6File.hasNext()) {
        //         String text = task6File.next();
        //         String[] ids = task6File.next().split(",");

        //         outputText += "{name: 'ID': ''" + text + "', children: [";
        //         for(String id : ids) {
        //             outputText += "{name: '"+ id +"'},";
        //         }
        //         outputText += "]}]};";
        //         System.out.println("Still working on task 6...");
        //     }
        //     outputText = outputText.substring(0, outputText.length()-1);
        //     outputText += "];";
        //     outputFile = new File("results/task6.js");
        //     output = new PrintWriter(outputFile);
        //     output.write(outputText);
        //     output.close();
        //     task6File.close();

        }
        catch(IOException ex) {
            System.out.println("There was an error accessing HDFS:\n" + ex.getMessage());
        }
        
    }
}
