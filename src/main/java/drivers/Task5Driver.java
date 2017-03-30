package drivers;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task5Driver {
    private static Task5Driver _instance;

    public static boolean run(String inputPath, String outputPath) {
        if(_instance == null)
            _instance = new Task5Driver();

        try {
            Job job = new Job();
            job.setJarByClass(drivers.Task5Driver.class);
            job.setJobName("Task 5");

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(mappers.FindReplies.class);
            job.setReducerClass(reducers.FindTweetsReducer.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            return job.waitForCompletion(true);
        }
        catch(Exception ex) {
            System.out.println("There was an error completing Task 5.");
            return false;
        }
    }
}
