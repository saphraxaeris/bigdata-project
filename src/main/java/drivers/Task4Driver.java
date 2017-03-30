package drivers;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task4Driver {
    private static Task4Driver _instance;

    public static boolean run(String inputPath, String outputPath) {
        if(_instance == null)
            _instance = new Task4Driver();

        try {
            Job job = Job.getInstance();
            job.setJarByClass(drivers.Task4Driver.class);
            job.setJobName("Task 4");

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(mappers.FindRetweetsMapper.class);
            job.setReducerClass(reducers.FindTweetsReducer.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            return job.waitForCompletion(true);
        }
        catch(Exception ex) {
            System.out.println("There was an error completing Task 4:\n" + ex.getMessage());
            return false;
        }
    }
}
