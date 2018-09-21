package part1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class CountryDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	          if (otherArgs.length != 4) {
		        System.err.println("Usage: CountyDriver <in> <out> <country1> <country2>");
		        System.exit(2);
		  }
		conf.set("country1", otherArgs[2].toUpperCase());
		conf.set("country2", otherArgs[3].toUpperCase());
		Job job = new Job(conf, "country category inverted list");
		job.setNumReduceTasks(3); // use three reducers
		job.setJarByClass(CountryDriver.class);
		job.setMapperClass(CountryMapper.class);
		job.setCombinerClass(CountryCombiner.class);
		job.setReducerClass(CountryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
