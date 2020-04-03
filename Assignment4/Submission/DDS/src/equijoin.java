
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
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Mapping extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Long joinKey = Long.parseLong(value.toString().replaceAll("\\s", "").split(",")[1]);
			context.write(new LongWritable(joinKey), value);
		}

	}

	public static class Reducing extends Reducer<LongWritable, Text, LongWritable, Text> {

		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> mapOutput = new ArrayList<String>();
			for (Text val : values) {
				mapOutput.add(val.toString());
			}

			Text output = new Text();
			for (int i = 0; i < mapOutput.size() - 1; i++) {
				for (int j = i + 1; j < mapOutput.size(); j++) {
					String table1 = mapOutput.get(i).split(",")[0].trim();
					String table2 = mapOutput.get(j).split(",")[0].trim();
					if (!table1.equals(table2)) {
						output.set(mapOutput.get(i) + ", " + mapOutput.get(j));
						context.write(null, new Text(output));
					}
				}
			}
		}
	}

}
