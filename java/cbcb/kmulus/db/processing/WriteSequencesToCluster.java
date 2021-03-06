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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class WriteSequencesToCluster  extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(WriteSequencesToCluster.class);
	
	private static final String USAGE = "WriteSequencesToCluster CLUSTER_INPUT SEQUENCE_INPUT BASE_OUTPUT_DIR OUTPUT_DIR NUM_CLUSTERS [NUM_TASKS]";
	
	public static final String LOG_DELIM = ",";
	public static final String SIMPLE_FASTA_SPLIT = " ";
	public static final String CLUSTER_SEPARATOR = "\t";
	public static final String BASE_OUTPUT_DIR = "BASE_OUTPUT_DIR";
	public static final String NUM_CLUSTERS = "NUM_CLUSTERS";
	
	private static final int MAX_REDUCES = 200;
	private static final int MAX_MAPS = 200;
	
	/**
	 * This mapper takes as input the cluster information and sequences, and outputs
	 * two possible pairs, (seqId, cluster it belongs to), and (seqId, sequence).
	 */
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			if (line.isEmpty())
				return;
			
			// Is the current line of a cluster?
			if (!line.contains(">")) {
				// For input 178 88, emit (178, 88)
				String[] tuple = line.split("\t");
				context.write(new LongWritable(Long.parseLong(tuple[0])), new Text(tuple[1]));
				
			} else {
				// TODO(cmhill) Error check.
				// For the sequence ">118 acgghachfcg", emit (118, '>118 acgg...')
				int spaceIndex = line.indexOf(" ");
				
				// Skip the header character.
				LongWritable seqId = new LongWritable(new Integer(line.substring(1, spaceIndex)));
				context.write(seqId, value);
			}
		}
	}
	
	/**
	 * This reducer receives a sequence Id, and the cluster and sequence to which it belongs.
	 * (cluster_id, sequence) is emitted.
	 */
	public static class ClusterSequencePairReduce extends Reducer<LongWritable, Text, LongWritable, Text>   {

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String sequence = null;
			long clusterId = -1L;
			
			// Get the sequence and which cluster this sequence belongs to.
			String line = null;
			int count = 0;
			for (Text value : values) {
				line = value.toString();
				
				if (line.contains(" ")) {
					sequence = line;
				} else {
					clusterId = Long.parseLong(value.toString());
				}
				count++;
			}
			if (count != 2) {
				throw new IOException("Sequence '" + key.get() + "' had " + count + " reduce "
						+ "values. Expected 2 (one for the sequence, one for the cluster).");
			}
			
			context.write(new LongWritable(clusterId), new Text(sequence));
		}
	}
	
	public static void main(String[] args) {
		int result = 1;

		try {
			result = ToolRunner.run(new WriteSequencesToCluster(), args);
			result = ToolRunner.run(new WriteClusterSequencesToHDFS(), args);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Job failed.");
		}
		System.exit(result);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		if(args.length < 5) {
			System.out.println(USAGE);
			return -1;
		}
		
		String clusterInputPath = args[0];
		String sequenceInputPath = args[1];
		String baseOutputPath = args[2];
		String outputDir = args[3];
		int numberOfClusters = new Integer(args[4]);
		
		LOG.info("Tool name: WriteSequencesToCluster");
		LOG.info(" - clusterInputDir: " + clusterInputPath);
		LOG.info(" - sequenceInputDir: " + sequenceInputPath);
		LOG.info(" - baseOutputPath: " + baseOutputPath);
		LOG.info(" - numberOfClusters: " + numberOfClusters);
		
		Job job = new Job(getConf(), "WriteSequencesToCluster");
		job.setJarByClass(WriteSequencesToCluster.class);

		job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048M");
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(WriteSequencesToCluster.Map.class);
		job.setReducerClass(WriteSequencesToCluster.ClusterSequencePairReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(clusterInputPath));
		FileInputFormat.addInputPath(job, new Path(sequenceInputPath));
		
		// Useless output directory.
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		int mapTasks = MAX_MAPS;
		int reduceTasks = MAX_REDUCES;

		if(args.length > 5) {
			int numTasks = Integer.parseInt(args[5]);
			mapTasks = numTasks;
			reduceTasks = numTasks;
		}
		
		job.setNumReduceTasks(reduceTasks);

		/* Setup the key value pairs */
		job.getConfiguration().set(BASE_OUTPUT_DIR, baseOutputPath);
		job.getConfiguration().setInt(NUM_CLUSTERS, numberOfClusters);
		
		// Delete the output directory if it exists already.
		FileSystem.get(job.getConfiguration()).delete(new Path(outputDir), true);

		long startTime = System.currentTimeMillis();

		boolean result = job.waitForCompletion(true);

		LOG.info((System.currentTimeMillis() - startTime) + LOG_DELIM
				+ mapTasks + LOG_DELIM + reduceTasks);

		return result ? 0 : 1;
	}
}
