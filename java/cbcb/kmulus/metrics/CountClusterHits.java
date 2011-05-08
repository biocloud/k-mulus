package cbcb.kmulus.metrics;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cbcb.kmulus.blast.BlastMapper;

/** Hadoop program which counts the number of sequences which are mapped to each cluster. */
public class CountClusterHits extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(CountClusterHits.class);
	private static final String USAGE = "Blast SEQUENCE_INPUT CLUSTER_INPUT OUTPUT [NUM_TASKS]";
	
	protected static final String ALPHABET_SIZE = "ALPHABET_SIZE";
	protected static final String BLAST_DATABASES = "BLAST_DATABASES";
	protected static final String CLUSTER_DIR = "CLUSTER_DIR";
	protected static final String HEADER_SEQUENCE_SEPARATOR = " ";
	protected static final String KMER_LENGTH = "KMER_LENGTH";
	
	private static final int MAX_REDUCES = 200;
	private static final int MAX_MAPS = 200;

	public static final String LOG_DELIM = ",";
	
	public static void main(String[] args) {
		int result = 1;
		try {
			result = ToolRunner.run(new CountClusterHits(), args);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Job failed.");
		}
		System.exit(result);
	}

	public int run(String[] args) throws Exception {

		if (args.length < 3) {
			System.out.println(USAGE);
			return -1;
		}

		String sequenceInputPath = args[0];
		String clusterInputPath = args[1];
		String outputPath = args[2];

		LOG.info("Tool name: " + CountClusterHits.class.getName());
		LOG.info(" - sequenceInputDir: " + sequenceInputPath);
		LOG.info(" - clusterInputDir: " + clusterInputPath);
		LOG.info(" - outputDir: " + outputPath);
		
		Job job = new Job(getConf(), CountClusterHits.class.getName());
		job.setJarByClass(CountClusterHits.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(BlastMapper.class);
		job.setReducerClass(CountReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(sequenceInputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		int mapTasks = MAX_MAPS;
		int reduceTasks = MAX_REDUCES;

		if(args.length > 3) {
			int numTasks = Integer.parseInt(args[3]);
			mapTasks = numTasks;
			reduceTasks = numTasks;	
		}
		
		job.setNumReduceTasks(reduceTasks);
		
		job.getConfiguration().set(CLUSTER_DIR, clusterInputPath);
		
		// Delete the output directory if it exists already.
		FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);
		
		long startTime = System.currentTimeMillis();
		boolean result = job.waitForCompletion(true);
		LOG.info((System.currentTimeMillis() - startTime) + 
				LOG_DELIM + mapTasks + LOG_DELIM + reduceTasks);

		return result ? 0 : 1;
	}
	
	public class CountReducer extends Reducer<LongWritable, Text, LongWritable, LongWritable> {
		
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (Text seq : values) {
				count++;
			}
			context.write(key, new LongWritable(count));
		}		
	}
}
