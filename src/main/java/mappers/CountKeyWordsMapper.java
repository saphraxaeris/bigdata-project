package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class CountKeyWordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String[] tweetWords = status.getText().toLowerCase().split(" ");

            for(String word : tweetWords) {
                if(word.contains("trump"))
                    context.write(new Text("Trump"), new IntWritable(1));
                else if(word.contains("maga"))
                    context.write(new Text("MAGA"), new IntWritable(1));
                else if(word.contains("dictator"))
                    context.write(new Text("Dictator"), new IntWritable(1));
                else if(word.contains("impeach"))
                    context.write(new Text("Impeach"), new IntWritable(1));
                else if(word.contains("drain"))
                    context.write(new Text("Drain"), new IntWritable(1));
                else if(word.contains("swamp"))
                    context.write(new Text("Swamp"), new IntWritable(1));
                else if(word.contains("change"))
                    context.write(new Text("Change"), new IntWritable(1));
            }
        }
        catch(TwitterException e) { /* Do nothing */ }
    }
}
