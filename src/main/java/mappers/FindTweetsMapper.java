package mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import java.io.IOException;

public class FindTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            
            String screenName = status.getUser().getScreenName();
            long tweetId = status.getId();
        
            context.write(new Text(screenName), new Text(tweetId + ""));
        }
        catch(TwitterException e) { /* Do nothing */ }
    }
}