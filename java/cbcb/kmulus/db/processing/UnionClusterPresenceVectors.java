package cbcb.kmulus.db.processing;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cbcb.kmulus.util.PresenceVector;

public class UnionClusterPresenceVectors extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(UnionClusterPresenceVectors.class);

	private static final String USAGE = "UnionClusterPresenceVectors CLUSTER_PRESENCE_VECTORS OUTPUT [NUM_TASKS]";
	protected static final String KMER_LENGTH = "KMER_LENGTH";
	
	private static final int MAX_REDUCES = 200;
	private static final int MAX_MAPS = 200;

	public static final String LOG_DELIM = ",";
	
	/**
	 * This mapper takes as input the cluster information and sequences, and outputs
	 * two possible pairs, (seqId, cluster it belongs to), and (seqId, sequence).
	 */
	public static class Map extends Mapper<LongWritable, Text, LongWritable, PresenceVector> {

		/**
		 * Replace with identity mapper.
		 */
		public void map(LongWritable key, PresenceVector value, Context context) 
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	/**
	 * Union all Presence vectors associated with a given cluster id.
	 */
	public static class Reduce extends Reducer<LongWritable, PresenceVector, LongWritable, PresenceVector> {
		
		private int kmerLength;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			kmerLength = context.getConfiguration().getInt(KMER_LENGTH, 3);
		}
		
		public void reduce(LongWritable key, Iterable<PresenceVector> values, Context context) 
			throws IOException, InterruptedException {
			
			PresenceVector clusterPV = new PresenceVector(kmerLength);
			
			for (PresenceVector pv : values) {
				clusterPV = clusterPV.union(pv);
			}
			
			context.write(key, clusterPV);
		}
	}
	
	public static void main(String[] args) {
		int result = 1;
		try {
			result = ToolRunner.run(new UnionClusterPresenceVectors(), args);
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

		String clusterInputPath = args[0];
		String outputPath = args[1];
		String kmerLen = args[2];

		LOG.info("Tool name: UnionClusterPresenceVectors");
		LOG.info(" - clusterInputDir: " + clusterInputPath);
		LOG.info(" - outputDir: " + outputPath);
		
		
		Job job = new Job(getConf(), "UnionClusterPresenceVectors");
		
		job.getConfiguration().setInt(KMER_LENGTH, Integer.parseInt(kmerLen));
		
		job.setJarByClass(UnionClusterPresenceVectors.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PresenceVector.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PresenceVector.class);
		
		job.setMapperClass(UnionClusterPresenceVectors.Map.class);
		job.setReducerClass(UnionClusterPresenceVectors.Reduce.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(clusterInputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		int mapTasks = MAX_MAPS;
		int reduceTasks = MAX_REDUCES;

		if(args.length > 2) {
			int numTasks = Integer.parseInt(args[2]);
			mapTasks = numTasks;
			reduceTasks = numTasks;	
		}
		
		job.setNumReduceTasks(reduceTasks);
		
		// Delete the output directory if it exists already.
		FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);
		
		long startTime = System.currentTimeMillis();

		boolean result = job.waitForCompletion(true);
		
		LOG.info((System.currentTimeMillis() - startTime) + 
				LOG_DELIM + mapTasks + LOG_DELIM + reduceTasks);

		return result ? 0 : 1;
	}
}