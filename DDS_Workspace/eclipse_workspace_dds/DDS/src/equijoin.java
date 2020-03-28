package map_reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {

	public static void main(String[] args)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "equijoin");
		job.setJarByClass(equijoin.class);
		job.setMapperClass(Mapping.class);
		job.setReducerClass(Reducing.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Mapping extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("In Map 8420355245");
			Long joinKey = Long.parseLong(value.toString().replaceAll("\\s", "").split(",")[1]);
			context.write(new LongWritable(joinKey), value);
			System.out.println("In Map Success 8420355245");

			
		}

	}

	public static class Reducing extends Reducer<Text, Text, Text, Text> {

		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("In Reduce 8420355245");
			
			List<String> mapOutput = new ArrayList<>();
			for(Text val : values) {
				mapOutput.add(val.toString());
			}
			
			System.out.println(mapOutput);
			
			System.out.println("In Reduce Success 8420355245");
		}
	}

}
