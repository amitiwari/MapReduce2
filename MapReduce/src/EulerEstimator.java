import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Random;



public class EulerEstimator extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			int seed=fileName.hashCode()+ key.hashCode();
			Random genRandom= new Random(seed);


			long count=0;         			

			long itr = Long.parseLong(value.toString());
			for(int i=1;i<=itr;i++) {
				double sum=0.0;
				while(sum<1){
					sum += genRandom.nextDouble();
					++count;

				}

			}

			context.getCounter("Euler", "iterations").increment(itr);
			context.getCounter("Euler", "count").increment(count);

		}
	}


	/*public static class IntSumReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }*/



	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new EulerEstimator(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Euler Estimator");
		job.setJarByClass(EulerEstimator.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		/*job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);*/

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class); 
		job.setOutputFormatClass(NullOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		//TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}


