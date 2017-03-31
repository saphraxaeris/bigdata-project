package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import java.io.IOException;
import java.util.regex.*;

public class FindUsernamesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String username = status.getUser().getScreenName();
            Pattern pt = Pattern.compile("[^a-zA-Z0-9]");
            Matcher match= pt.matcher(username);
            while(match.find())
            {
                String s= match.group();
                username=username.replaceAll("\\"+s, "");
            }
            context.write(new Text(username), new IntWritable(1));
        }
        catch(TwitterException e) { /* Do nothing */ }
    }
}