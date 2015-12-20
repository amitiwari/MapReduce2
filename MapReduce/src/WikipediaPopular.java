
	import java.io.IOException;
	import java.text.BreakIterator;
	import java.text.Normalizer;
	import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.conf.Configured;
	import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
	//import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
	import org.apache.hadoop.util.Tool;
	import org.apache.hadoop.util.ToolRunner;



public class WikipediaPopular extends Configured implements Tool {
	
	public static class TokenizerMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 
//        private final static LongWritable one = new LongWritable(1);
        LongWritable long1 = new LongWritable(1);
        private Text word = new Text();
        
        
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
  	 	  	
        	
        	
        	
        		 String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
        		 String fkey[]  =  fileName.split("-");
        		String fileKey = fkey[1]+"-"+ fkey[2].substring(0,2); 
        		String line = value.toString();
        		String []testString = line.split("\\s+");
        		
        	if(testString[0].equals(new String("en")) && (!(testString[1].startsWith("Special:") || testString[1].contains("Main_Page"))))
        		     		  
        		long1 = new LongWritable(Long.parseLong(testString[2])) ; 
        	
        	  
        	 
        	  	Text keyName = new Text(fileKey);
        	  	context.write(keyName,long1); 
            }
        	
        	
        	
        	
        	
        	}
        
	public static class LongMaxReducer
    extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();
 
        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context
                ) throws IOException, InterruptedException {
        	long max = 0;
            for (LongWritable val : values) {
                   	if(val.get()>max)
            		max=val.get();
            }
            result.set(max);
            context.write(key, result);
        }
    }
 
        
    
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Wikipedia Popular");
        job.setJarByClass(WikipediaPopular.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(LongMaxReducer.class);
        job.setReducerClass(LongMaxReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
	

}
