package cbcb.kmulus.blast;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

/**
 * Hadoop program where the mapper determines which clusters overlap with a given
 * query sequence.  The reducer then runs BLASTS on those sequences that overlap 
 * with a given cluster, using the cluster sequences has the blast database.
 */
public class Blast extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Blast.class);

	private static final String USAGE = "Blast SEQUENCE_INPUT CLUSTER_INPUT OUTPUT NUM_CENTERS PARTITION_URI [KMER LENGTH] [NUM_TASKS]";
	
	public static final String ALPHABET_SIZE = "ALPHABET_SIZE";
	public static final String BLAST_DATABASES = "BLAST_DATABASES";
	public static final String CLUSTER_DIR = "CLUSTER_DIR";
	public static final String NUM_CENTERS = "NUM_CENTERS";
	public static final String KMER_LENGTH = "KMER_LENGTH";
	
	protected static final String HEADER_SEQUENCE_SEPARATOR = " ";
	
	private static final int MAX_REDUCES = 200;
	private static final int MAX_MAPS = 200;

	public static final String LOG_DELIM = ",";
	
	public static void main(String[] args) {
		int result = 1;
		try {
			result = ToolRunner.run(new Blast(), args);
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

		LOG.info("Tool name: Blast");
		LOG.info(" - sequenceInputDir: " + sequenceInputPath);
		LOG.info(" - clusterInputDir: " + clusterInputPath);
		LOG.info(" - outputDir: " + outputPath);
		
		Job job = new Job(getConf(), "Blast");
		job.setJarByClass(Blast.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(BlastMapper.class);
		job.setReducerClass(BlastReducer.class);
		
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
		job.getConfiguration().setInt(NUM_CENTERS, numCenters);
		job.getConfiguration().setInt(KMER_LENGTH, 3);
		
		if(args.length > 5){
			job.getConfiguration().setInt(KMER_LENGTH, Integer.parseInt(args[5]));
		}

		// TODO(cmhill): Remove since, it's always using this symlink.
		job.getConfiguration().set(BLAST_DATABASES, "blastdbs");
		
		// Add the blastdbs to the DistributedCache.
		URI partitionUri = new URI(args[4] + "#blastdbs"); 
		DistributedCache.addCacheArchive(partitionUri, job.getConfiguration()); 
		DistributedCache.createSymlink(job.getConfiguration());
		
		// Delete the output directory if it exists already.
		FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);
		
		long startTime = System.currentTimeMillis();

		boolean result = job.waitForCompletion(true);
		
		LOG.info((System.currentTimeMillis() - startTime) + 
				LOG_DELIM + mapTasks + LOG_DELIM + reduceTasks);

		return result ? 0 : 1;
	}
}
