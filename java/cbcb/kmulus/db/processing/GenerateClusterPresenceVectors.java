package cbcb.kmulus.db.processing;

import cbcb.kmulus.util.Biology;
import cbcb.kmulus.util.PresenceVector;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class GenerateClusterPresenceVectors extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(GenerateClusterPresenceVectors.class);
	
	protected static final String CLUSTER_SEPARATOR = "\t";
	protected static final String SEQUENCE_SEPARATOR = " ";

	protected static final String USAGE = "GenerateClusterPresenceVectors CLUSTER_DIRECTOR SEQUENCE_INPUT_DIR OUTPUT [NUM_TASKS]";
	protected static final String KMER_LENGTH = "KMER_LENGTH";
	protected static final String LOG_DELIM = ",";

	private static final int MAX_REDUCES = 200;	  
	private static final int MAX_MAPS = 200;
	
	/**
	 * This mapper takes as input the cluster information and simple fasta
	 * sequences, and emit (sequence id, cluster id) for clusters, and emits
	 * (sequence id, sequence) for sequences.
	 */
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

		private int kmerLength;

		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kmerLength = conf.getInt(GenerateClusterPresenceVectors.KMER_LENGTH, 3);
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			if (line.isEmpty())
				return;

			// Are we looking at a cluster input or sequences?
			if (line.contains(GenerateClusterPresenceVectors.CLUSTER_SEPARATOR)) {

				String[] clusterAndMembers = line.split(GenerateClusterPresenceVectors.CLUSTER_SEPARATOR);
				LongWritable cluster = new LongWritable(new Long(
						clusterAndMembers[0]));

				// For sequence that is a member of this cluster, emit (seqId,
				// clusterId).
				for (int i = 1; i < clusterAndMembers.length; i++) {
					context.write(cluster, new Text(clusterAndMembers[i]));
				}
			} else {
				// TODO(cmhill) Error check.
				// For the sequence ">118 acgghachfcg", emit (118, acgghachfcg).
				int spaceIndex = line.indexOf(" ");

				// Skip the header character.
				LongWritable seqId = new LongWritable(new Integer(line.substring(1,
						spaceIndex)));
				String sequence = line.substring(spaceIndex + 1);

				context.write(seqId, new Text(sequence));
			}
		}
	}
	
	/**
	 * For each sequence id both the clusterId and the sequence will be received.
	 * Emit the (cluster id, sequence {@link PresenceVector}).
	 */
	public static class Reduce extends Reducer<LongWritable, Text, 
			LongWritable, PresenceVector> {

		private static int KMER_LENGTH = 3;
		
		@Override
		protected void setup(Context context) {
			KMER_LENGTH = context.getConfiguration().getInt(
					GenerateClusterPresenceVectors.KMER_LENGTH, 3);
		}
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException { 
			String sequence = null;
			LongWritable clusterId = null;
			
			String line = null;
			for (Text value : values) {
				line = value.toString();
				
				if (line.contains(GenerateClusterPresenceVectors.SEQUENCE_SEPARATOR)) {
					sequence = value.toString();
				} else
					clusterId = new LongWritable(new Long(value.toString()));
			}
			
			if (sequence != null && clusterId != null)
				emitPresenceVector(clusterId, sequence, context);
		}
		
		/**
		 * Emit the kmers from sequence: (cluster_id, {@link PresenceVector}).
		 * 
		 * @param clusterId cluster id
		 * @param sequence String of the sequence
		 * @param context
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		private void emitPresenceVector(LongWritable clusterId, String sequenceAndHeader, Context context)
				throws IOException, InterruptedException {
			
			String sequence = sequenceAndHeader.substring(sequenceAndHeader.indexOf(
					GenerateClusterPresenceVectors.SEQUENCE_SEPARATOR) + 1);
			
			PresenceVector pv = new PresenceVector(KMER_LENGTH);
			
			for (int i = 0; i <= sequence.length() - KMER_LENGTH; i++) {
				pv.setKmer(Biology.getAAHash(sequence.substring(i, i + KMER_LENGTH)));
			}
			
			context.write(clusterId, pv);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 3) {
			System.out.println(USAGE);
			return -1;
		}

		String clusterDirPath = args[0];
		String sequenceInputPath = args[1];
		String outputPath = args[2];
		
		LOG.info("Tool name: GenerateClusterPresenceVectors");
		LOG.info(" - clusterDirectory: " + clusterDirPath);
		LOG.info(" - sequenceInputDir: " + sequenceInputPath);
		LOG.info(" - outputDir: " + outputPath);

		Job job = new Job(getConf(), "GenerateClusterPresenceVectors");
		job.setJarByClass(GenerateClusterPresenceVectors.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PresenceVector.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(GenerateClusterPresenceVectors.Map.class);
		job.setReducerClass(GenerateClusterPresenceVectors.Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(sequenceInputPath));
		FileInputFormat.addInputPath(job, new Path(clusterDirPath));

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		int mapTasks = MAX_MAPS;
		int reduceTasks = MAX_REDUCES;
		
		/* Setup the key value pairs */
		job.getConfiguration().setInt(KMER_LENGTH, 3);
		
		if (args.length > 2) {			
			job.getConfiguration().setInt(KMER_LENGTH, new Integer(args[2]));
			
			if (args.length > 3) {
				int numTasks = Integer.parseInt(args[3]);
				mapTasks = numTasks;
				reduceTasks = numTasks;
			}
		}

		job.setNumReduceTasks(reduceTasks);

		// Delete the output directory if it exists already.
		FileSystem.get(job.getConfiguration()).delete(new Path(outputPath),
				true);

		long startTime = System.currentTimeMillis();

		boolean result = job.waitForCompletion(true);

		LOG.info((System.currentTimeMillis() - startTime) + LOG_DELIM
				+ mapTasks + LOG_DELIM + reduceTasks);

		return result ? 0 : 1;
	}
}
