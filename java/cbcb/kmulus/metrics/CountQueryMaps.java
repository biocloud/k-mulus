package cbcb.kmulus.metrics;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cbcb.kmulus.blast.Blast;

/** 
 * This program determines how many clusters a specific query hits.
 * 
 * @author cmhill / modified by sga
 * @see Blast
 */
public class CountQueryMaps extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(CountQueryMaps.class);
	private static final String USAGE = "SEQUENCE_INPUT CLUSTER_INPUT OUTPUT KMER_LENGTH NUM_CENTERS";
	
	protected static final String ALPHABET_SIZE = "ALPHABET_SIZE";
	protected static final String CLUSTER_DIR = "CLUSTER_DIR";
	protected static final String HEADER_SEQUENCE_SEPARATOR = " ";
	protected static final String KMER_LENGTH = "KMER_LENGTH";
	protected static final String NUM_CENTERS = "NUM_CENTERS";
	
	private static final int MAX_REDUCES = 100;
	private static final int MAX_MAPS = 100;

	public static final String LOG_DELIM = ",";
	
	public static void main(String[] args) {
		int result = 1;
		try {
			result = ToolRunner.run(new CountQueryMaps(), args);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Job failed.");
		}
		System.exit(result);
	}

	public int run(String[] args) throws Exception {

		if (args.length < 5) {
			System.out.println(USAGE);
			return -1;
		}

		String sequenceInputPath = args[0];
		String clusterInputPath = args[1];
		String outputPath = args[2];
		String kmerlength = args[3];
		String numCenters = args[4];
		int minKmerMatch = 1;
		if (args.length > 5) {
			minKmerMatch = Integer.parseInt(args[5]);
		}
		
		LOG.info("Tool name: " + CountQueryMaps.class.getName());
		LOG.info(" - sequenceInputDir: " + sequenceInputPath);
		LOG.info(" - clusterInputDir: " + clusterInputPath);
		LOG.info(" - outputDir: " + outputPath);
		LOG.info(" - kmerlength: " + kmerlength);
		LOG.info(" - numCenters: " + numCenters);
		LOG.info(" - minKmerMatch: " + minKmerMatch);
		
		Job job = new Job(getConf(), CountQueryMaps.class.getName());
		job.setJarByClass(CountQueryMaps.class);
		
		job.getConfiguration().setInt(KMER_LENGTH, Integer.parseInt(kmerlength));
		job.getConfiguration().setInt(NUM_CENTERS, Integer.parseInt(numCenters));
		job.getConfiguration().setInt(Blast.MIN_KMER_MATCH, minKmerMatch);
		
		job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setMapperClass(QueryCountMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(sequenceInputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		int mapTasks = MAX_MAPS;
		int reduceTasks = MAX_REDUCES;

		if(args.length > 6) {
			int numTasks = Integer.parseInt(args[6]);
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
}
