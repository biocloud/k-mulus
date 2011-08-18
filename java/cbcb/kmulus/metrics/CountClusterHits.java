package cbcb.kmulus.metrics;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cbcb.kmulus.blast.Blast;
import cbcb.kmulus.blast.BlastMapper;

/** 
 * Hadoop program which counts the number of sequences which are mapped to each cluster. Should only
 * be run after database clustering has been completed.
 * 
 * Emits values of the form (cluster_id, count), where count is the number of sequences which map
 * to that cluster center.
 * 
 * @see Blast
 */
public class CountClusterHits extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(CountClusterHits.class);
	private static final String USAGE = "CountClusterHits SEQUENCE_INPUT CLUSTER_INPUT OUTPUT NUM_CLUSTERS [NUM_TASKS]";
	
	protected static final String HEADER_SEQUENCE_SEPARATOR = " ";
	
	private static final int MAX_REDUCES = 100;
	private static final int MAX_MAPS = 100;

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

		if (args.length < 4) {
			System.out.println(USAGE);
			return -1;
		}

		String sequenceInputPath = args[0];
		String clusterInputPath = args[1];
		String outputPath = args[2];
		int numCenters = Integer.parseInt(args[3]);

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

		if (args.length > 4) {
			int numTasks = Integer.parseInt(args[4]);
			mapTasks = numTasks;
			reduceTasks = numTasks;
		}
		
		if (args.length > 5) {
			job.getConfiguration().setInt(Blast.KMER_LENGTH,
					Integer.parseInt(args[5]));
		}
		
		if (args.length > 6) {
			job.getConfiguration().setInt(Blast.MIN_KMER_MATCH,
					Integer.parseInt(args[6]));
		}
		
		job.setNumReduceTasks(reduceTasks);
		
		job.getConfiguration().set(Blast.CLUSTER_DIR, clusterInputPath);
		job.getConfiguration().setInt(Blast.NUM_CENTERS, numCenters);
		
		// Delete the output directory if it exists already.
		FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);
		
		long startTime = System.currentTimeMillis();
		boolean result = job.waitForCompletion(true);
		LOG.info((System.currentTimeMillis() - startTime) + 
				LOG_DELIM + mapTasks + LOG_DELIM + reduceTasks);

		return result ? 0 : 1;
	}
	
	/** Counts the number of sequences mapped to each cluster center. Emits (cluster_id, count). */
	public static class CountReducer extends Reducer<LongWritable, Text, LongWritable, LongWritable> {
		
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
