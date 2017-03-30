package drivers;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.mapreduce.Job;

import drivers.Task1Driver;
import drivers.Task2Driver;
import drivers.Task3Driver;
import drivers.Task4Driver;

public class MainDriver {
    private static boolean result1 = false;
    private static boolean result2 = false;
    private static boolean result3 = false;
    private static boolean result4 = false;
    private static boolean result5 = false;
    private static boolean result6 = false;

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MainDriver <input path> <output directory>");
            System.exit(-1);
        }
        
        boolean terminate = false;
        Scanner in = new Scanner(System.in);

        while(!terminate) {
            runTasks(args);
            if(result1 && result2 && result3 && result4 && result5 && result6)
                terminate = true;
            else {
                String temp = "";
                if(!result1)
                    temp += "task1, ";
                if(!result2)
                    temp += "task2, ";
                if(!result3)
                    temp += "task3, ";
                if(!result4)
                    temp += "task4, ";
                if(!result5)
                    temp += "task5, ";
                if(!result6)
                    temp += "task6, ";
                if(temp.length() > 0)
                    temp = temp.substring(0, temp.length()-3);
                System.out.println("\n\nRetry task(s): " + temp + "? (y/n)");
                boolean valid = false;
                String reply = "";
                while(!valid) {
                    reply = in.next();
                    if(reply.toLowerCase().matches("y") || reply.toLowerCase().matches("n"))
                        valid = true;
                }
                if(reply.toLowerCase().matches("n"))
                    terminate = true;
            }
        }
        in.close();

        System.out.println("\n\nTerminating...");
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void runTasks(String[] args) {
        try {
            Job job = Job.getInstance();
            if(!result1)
                result1 = Task1Driver.run(args[0], args[1] + "/task1", job);
            if(!result2)
                result2 = Task2Driver.run(args[0], args[1] + "/task2", job);
            if(!result3)
                result3 = Task3Driver.run(args[0], args[1] + "/task3", job);
            if(!result4)
                result4 = Task4Driver.run(args[0], args[1] + "/task4", job);
            if(!result5)
                result5 = Task5Driver.run(args[0], args[1] + "/task5", job);
            if(!result6)
                result6 = Task6Driver.run(args[0], args[1] + "/task6", job);
        }
        catch(IOException ex) {
            System.out.println("There was an error processing tasks.");
        }
    }
}
