package mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import java.io.IOException;

public class FindRepliesMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            long tweetId = status.getId();
            if(status.getInReplyToStatusId() > 0) {
                long originalTweetId = status.getInReplyToStatusId();
                context.write(new LongWritable(tweetId), new Text(originalTweetId + ""));
            }
        }
        catch(TwitterException e) { /* Do nothing */ }
    }
}