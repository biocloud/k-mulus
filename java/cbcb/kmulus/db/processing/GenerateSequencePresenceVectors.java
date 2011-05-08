package cbcb.kmulus.db.processing;

import java.io.IOException;

import cbcb.kmulus.util.Biology;
import cbcb.kmulus.util.PresenceVector;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Hadoop program for creating the k-mer presence vectors of the input sequences.
 */
public class GenerateSequencePresenceVectors extends Configured implements Tool {
	
		private static final Logger LOG = Logger.getLogger(GenerateSequencePresenceVectors.class);

		private static final String USAGE = "GenerateSequencePresenceVectors SEQUENCE_INPUT OUTPUT [KMER_LEN] [NUM_TASKS]";
		private static final String KMER_LENGTH = "KMER_LENGTH";
		private static final String LOG_DELIM = ",";

		private static final int MAX_REDUCES = 200;	  
		private static final int MAX_MAPS = 200;

		/**
		 * This mapper takes as input the cluster information and sequences, and outputs
		 * (seqId, {@link PresenceVector}).
		 */
		public static class Map extends Mapper<LongWritable, Text, LongWritable, PresenceVector> {
			
			private int kmerLength;
			
			@Override
			protected void setup (Context context) {
				Configuration conf = context.getConfiguration();
				kmerLength = conf.getInt(KMER_LENGTH, 3);
			}
			
			/**
			 * Creates a {@link PresenceVector} for the simple fasta sequence and emits:
			 * (sequence_id, [00101001001])
			 */
			public void map(LongWritable key, Text value, Context context) 
					throws IOException, InterruptedException {
				String line = value.toString().trim();
				if (line.isEmpty())
					return;
				
				// TODO(cmhill) Error check.
				// For the sequence ">118 acgghachfcg", emit (118, [0010101001...])
				int spaceIndex = line.indexOf(" ");
				
				// Skip the header character.
				LongWritable seqId = new LongWritable(new Integer(line.substring(1, spaceIndex)));
				String sequence = line.substring(spaceIndex + 1);
				
				// Create the feature vector.
				PresenceVector featureVector = new PresenceVector(kmerLength);
				featureVector.setId(seqId.get());
				
				for (int i = 0; i <= sequence.length() - kmerLength; i += kmerLength) {
					int hash = Biology.getAAKmerHash(sequence.substring(i, i + kmerLength));
					if (hash >= 0) {
						featureVector.setKmer(hash);
					}
				}		
					
				context.write(seqId, featureVector);
			}
		}
		
		/**
		 * OPTIONAL TODO(cmhill): 
		 * The reducer just prints the kmers found in all sequences.
		 */
		public static class Reduce extends Reducer<LongWritable, BytesWritable, LongWritable, Text> {
			
		}
	
	public static void main(String[] args) {
		int result = 1;

		try {
			result = ToolRunner.run(new GenerateSequencePresenceVectors(), args);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Job failed.");
		}
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.out.println(USAGE);
			return -1;
		}

		String sequenceInputPath = args[0];
		String outputPath = args[1];
		
		LOG.info("Tool name: GenerateSequencePresenceVectors");
		LOG.info(" - sequenceInputDir: " + sequenceInputPath);
		LOG.info(" - outputDir: " + outputPath);

		Job job = new Job(getConf(), "GenerateSequencePresenceVectors");
		job.setJarByClass(GenerateSequencePresenceVectors.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PresenceVector.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PresenceVector.class);

		job.setMapperClass(GenerateSequencePresenceVectors.Map.class);
		// job.setReducerClass(GenerateSequencePresenceVectors.Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(sequenceInputPath));
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
