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

import cbcb.kmulus.db.cluster.ClusterPresenceVectors;
import cbcb.kmulus.util.PresenceVector;

/**
 * Prepares the output of the clustering stage {@link ClusterPresenceVectors} for the database
 * partitioning to be done by {@link WriteSequencesToCluster}.
 * 
 * @author CH Albach
 */
public class PrepareClusteringOutput extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(PrepareClusteringOutput.class);

	private static final String USAGE = "PrepareClusteringOutput CLUSTER_PRESENCE_VECTORS OUTPUT [NUM_TASKS]";
	protected static final String KMER_LENGTH = "KMER_LENGTH";
	
	private static final int MAX_REDUCES = 200;
	private static final int MAX_MAPS = 200;

	public static final String LOG_DELIM = ",";
	
	/**
	 * This mapper takes as input the (cluster_id, presence_vector), and outputs
	 * (seqId, cluster_id)
	 */
	public static class Map extends Mapper<LongWritable, PresenceVector, LongWritable, LongWritable> {

		/**
		 * Replace with identity mapper.
		 */
		@Override
		public void map(LongWritable key, PresenceVector value, Context context) 
				throws IOException, InterruptedException {
			context.write(new LongWritable(value.getId()), key);
		}
	}
	
	public static void main(String[] args) {
		int result = 1;
		try {
			result = ToolRunner.run(new PrepareClusteringOutput(), args);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Job failed.");
		}
		System.exit(result);
	}

	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.out.println(USAGE);
			return -1;
		}

		String clusterInputPath = args[0];
		String outputPath = args[1];

		LOG.info("Tool name: " + PrepareClusteringOutput.class.getName());
		LOG.info(" - clusterInputDir: " + clusterInputPath);
		LOG.info(" - outputDir: " + outputPath);
		
		Job job = new Job(getConf(), PrepareClusteringOutput.class.getName());
		job.setJarByClass(PrepareClusteringOutput.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PresenceVector.class);
		
		job.setMapperClass(PrepareClusteringOutput.Map.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(clusterInputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		int mapTasks = MAX_MAPS;

		if(args.length > 2) {
			int numTasks = Integer.parseInt(args[2]);
			mapTasks = numTasks;
		}
		
		// Delete the output directory if it exists already.
		FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);
		
		long startTime = System.currentTimeMillis();

		boolean result = job.waitForCompletion(true);
		
		LOG.info((System.currentTimeMillis() - startTime) + 
				LOG_DELIM + mapTasks);

		return result ? 0 : 1;
	}
}