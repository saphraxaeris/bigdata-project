package reducers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FindUsersTweetsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String list = "";
        for(Text id : values) {
            list += id.toString() + ",";
        }
        list.substring(0, list.length()-3);
        context.write(key, new Text(list));
    }
}
