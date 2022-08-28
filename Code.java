package assignment2.ex3;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Recommendation extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new Recommendation(), args);
		System.exit(res);
	}

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        Job job = new Job(getConf(), "Recommendation");
        job.setJarByClass(Recommendation.class);
      
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
      
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

		return 0;
    }

	public static class Map extends Mapper<Text, Text, IntWritable, Text> {
		public final static IntWritable ZERO = new IntWritable(0);	// 0 means 1st user (current user) and 2nd user are friends already
		public final static IntWritable ONE = new IntWritable(1);	// 1 means 1st user and 2nd user are mutual friend of current user

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if (value.getLength() != 0) {
				String[] friendList = value.toString().split(",");
				for (int i = 0; i < friendList.length; i ++) {
				// Record all "friends already" pair, so we would not recommend this the 1st user to 2nd user, or vice versa
					context.write(new IntWritable(Integer.parseInt(key.toString())), new Text(friendList[i] + " " + ZERO));
			   
					for (int j = i + 1; j < friendList.length; j++) {
						// Record "mutual friends" pair, and would calculate number of mutual friends in the reduce process
						context.write(new IntWritable(Integer.parseInt(friendList[i])), new Text(friendList[j] + " " + ONE));
						context.write(new IntWritable(Integer.parseInt(friendList[j])), new Text(friendList[i] + " " + ONE));
					}	
				}
			} 
		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {	
		
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			HashMap<String, Integer> recommendation = new HashMap<String, Integer>();
			for(Text value : values){
				String[] output = value.toString().split(" ");	
				//update the frequencies of having mutual friend between current users and other users
				if(Integer.parseInt(output[1]) == 0){
					recommendation.put(output[0], 0);
				}
				else if(Integer.parseInt(output[1]) == 1){
					if(recommendation.containsKey(output[0])){
						if(recommendation.get(output[0]) != 0){
							recommendation.put(output[0], recommendation.get(output[0]) + 1);
						}
					}
					else{
						recommendation.put(output[0], 1);
					}
				}
			}
			//comparator which sort the entries in descending order of the values and only store at most 10 users
			PriorityQueue<Entry<String, Integer>> recommended10 = new PriorityQueue<Entry<String, Integer>>(10, new Comparator<Entry<String, Integer>>(){
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2){
					return o2.getValue().compareTo(o1.getValue());
				}
			});
			for(Entry<String, Integer> entry : recommendation.entrySet()){
				if(!entry.getValue().equals(0)){
					recommended10.add(entry);
				}
			}
			int friendCount = 0;
			String results = "";
			while(!recommended10.isEmpty()){
				results = results + recommended10.poll().getKey() + ",";
				friendCount++;
				if(friendCount == 10){
					break;
				}
			}
			if(results.length() > 0){
				context.write(key, new Text(results.substring(0, results.length() - 1)));
			}
		}
	}
}
