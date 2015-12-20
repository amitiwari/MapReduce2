	import java.io.IOException;
	import java.util.StringTokenizer;
	 
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.conf.Configured;
	import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
	import org.apache.hadoop.util.Tool;
	import org.apache.hadoop.util.ToolRunner;
	import com.fasterxml.jackson.databind.JsonNode;
	import com.fasterxml.jackson.databind.ObjectMapper;

	 
public class RedditAverage extends Configured implements Tool{

	public static class TokenizerMapper
    extends Mapper<LongWritable, Text, Text, LongPairWritable>{
 
           
        
        
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	        	
        	ObjectMapper json_mapper = new ObjectMapper();
            JsonNode data= json_mapper.readValue(value.toString(), JsonNode.class);
           
            String subreddit = data.get("subreddit").textValue();
            Long score = data.get("score").longValue();
            
            LongPairWritable pair = new LongPairWritable();
            pair.set(1, score);
           
            context.write(new Text(subreddit), pair);
               
        	           
                
           
        }
    }
	
	public static class RedditAverageCombiner
    extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		LongPairWritable combine = new LongPairWritable();
		
        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
        	long combineComments = 0;
            long combineScore = 0;
            for (LongPairWritable val : values) {
            	combineComments += val.get_0();
            	combineScore += val.get_1();
            	            	
            }
            combine.set(combineComments, combineScore);
            context.write(key, combine);
            
        }
    }
 
    public static class RedditAverageReducer
    extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        
        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
        	 Double average = 0.0;
             long combineComments = 0;
             Double combineScore = 0.0;
            for (LongPairWritable val : values) {
            	combineComments += val.get_0();
            	combineScore += val.get_1();
            	
            }
            average+=combineScore/combineComments;
            result.set(average);
            context.write(key, result); 
        }
    }
    
    
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Reddit Average");
        job.setJarByClass(RedditAverage.class);
       
 
        job.setInputFormatClass(MultiLineJSONInputFormat.class);
 
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(RedditAverageCombiner.class);
        job.setReducerClass(RedditAverageReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongPairWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultiLineJSONInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }

	
}
