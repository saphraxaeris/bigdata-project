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
            String[] tweetWords = status.getText().toLowerCase().replace("#", "").split(" ");

            for(String word : tweetWords) {
                if(word.equals("trump"))
                    context.write(new Text("Trump"), new IntWritable(1));
                else if(word.equals("maga"))
                    context.write(new Text("MAGA"), new IntWritable(1));
                else if(word.equals("dictator"))
                    context.write(new Text("Dictator"), new IntWritable(1));
                else if(word.equals("impeach"))
                    context.write(new Text("Impeach"), new IntWritable(1));
                else if(word.equals("drain"))
                    context.write(new Text("Drain"), new IntWritable(1));
                else if(word.equals("swamp"))
                    context.write(new Text("Swamp"), new IntWritable(1));
                else if(word.equals("change"))
                    context.write(new Text("Change"), new IntWritable(1));
            }
        }
        catch(TwitterException e) { /* Do nothing */ }
    }
}