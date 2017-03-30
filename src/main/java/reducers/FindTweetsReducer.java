package reducers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FindTweetsReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        String list = "";
        for(LongWritable id : values) {
            list += id.toString() + ", ";
        }
        list.substring(0, list.length()-3);
        context.write(key, new Text(list));
    }
}
