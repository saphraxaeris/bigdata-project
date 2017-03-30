package drivers;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task1Driver {
    private static Task1Driver _instance;

    public static boolean run(String inputPath, String outputPath) {
        if(_instance == null)
            _instance = new Task1Driver();

        try {
            Job job = Job.getInstance();
            job.setJarByClass(drivers.Task1Driver.class);
            job.setJobName("Task 1");

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(mappers.CountKeyWordsMapper.class);
            job.setReducerClass(reducers.CountWordsReducer.class);
            job.setCombinerClass(reducers.CountWordsReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            return job.waitForCompletion(true);
        }
        catch(Exception ex) {
            System.out.println("There was an error completing Task 1:\n" + ex.getMessage());
            return false;
        }
    }
}
