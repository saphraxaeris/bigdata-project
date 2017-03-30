package drivers;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task3Driver {
    private static Task3Driver _instance;

    public static boolean run(String inputPath, String outputPath, Job job) {
        if(_instance == null)
            _instance = new Task3Driver();

        try {
            job.setJarByClass(drivers.Task3Driver.class);
            job.setJobName("Task 3");

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(mappers.FindUsernamesMapper.class);
            job.setReducerClass(reducers.CountWordsReducer.class);
            job.setCombinerClass(reducers.FindUsernamesReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            return job.waitForCompletion(true);
        }
        catch(Exception ex) {
            System.out.println("There was an error completing Task 3:\n" + ex.getMessage());
            return false;
        }
    }
}
