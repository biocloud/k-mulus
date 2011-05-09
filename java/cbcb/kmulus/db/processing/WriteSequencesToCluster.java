package cbcb.kmulus.db.processing;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cbcb.kmulus.util.PresenceVector;

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
				// For cluster "88	178:142:39:...", emit (178, 88), (142, 88), (39, 88) ...
				String[] tuple = line.split("\t");
				
				Text clusterCenter = new Text(tuple[0]);
				try {
					for (int i = 1; i < tuple.length; i++) {
						context.write(new LongWritable(new Long(tuple[i])), clusterCenter);
					}
				} catch (java.lang.ArrayIndexOutOfBoundsException e) {
					throw new IOException("Failure on line:" + line);
				}
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
	 * During the reduce phase, the sequence is written to the base_directory/cluster_id/sequence.
	 */
	public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text>   {
		private FileSystem hdfs;
		private String baseOutputDir;
		private boolean failedSetup = false;
		private int numberOfClusters;

		/* Each key is the cluster center and value is the outputWriter to the 
		respective file: [BASE_OUTPUT_DIR]/[CLUSTER_ID]/seq.[RANDOM_NUMBER] */
		private HashMap<Long, BufferedWriter> outputWriters;

		private int unique_id;
		
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			
			baseOutputDir = conf.get(BASE_OUTPUT_DIR);
			numberOfClusters = conf.getInt(NUM_CLUSTERS, 1);
			
			outputWriters = new HashMap<Long, BufferedWriter>();
			
			// TODO(cmhill): Better way to get a "unique" number for the reducer.
			Random rand = new Random();
			unique_id = rand.nextInt(10000000);
			
			try {
				hdfs = FileSystem.get(conf);
			} catch (IOException e) {
				failedSetup = true;
			}
		}
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String sequence = null;
			LongWritable clusterId = null;
			
			// Get the sequence and which cluster this sequence belongs to.
			String line = null;
			for (Text value : values) {
				line = value.toString();
				
				if (line.contains(" ")) {
					sequence = line;
				} else
					clusterId = new LongWritable(new Long(value.toString()));
			}
			
			// Output the sequence in fasta format to the correct hdfs directory.
			if (sequence != null && clusterId != null) {
				BufferedWriter bw = getBufferedWriterForCluster(clusterId.get());
				String headerAndSequence[] = sequence.split(SIMPLE_FASTA_SPLIT);
				// TODO(cmhill): Split the sequence into 60 character chunks. 
				bw.write(headerAndSequence[0] + "\n" + headerAndSequence[1] + "\n");		
			}			
		}
		
		/**
		 * Return the BufferedWriter for the hdfs file of the cluster id.
		 * 
		 * @param clusterId
		 * @return BufferedWriter for the hdfs file "[BASE_OUTPUT_DIR]/[CLUSTER_ID]/seq.[RANDOM_NUMBER]"
		 * @throws IOException
		 */
		private BufferedWriter getBufferedWriterForCluster(long clusterId) throws IOException {
			if (outputWriters.get(clusterId) == null) {
				FSDataOutputStream outputStream = hdfs.create(new Path(baseOutputDir + "/" + clusterId + "/seq." + unique_id));
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
				
				outputWriters.put(clusterId, writer);
				
				return writer;
			} else {
				return outputWriters.get(clusterId);
			}
		}
		
		/**
		 * Close all BufferedWriters.
		 */
		@Override
		protected void cleanup (Context context) {
			for (long key : outputWriters.keySet()) {
				try {
					outputWriters.get(key).close();
				} catch (IOException e) {
					LOG.info("Error during reducer clean up");
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args) {
		int result = 1;

		try {
			result = ToolRunner.run(new WriteSequencesToCluster(), args);

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

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(WriteSequencesToCluster.Map.class);
		job.setReducerClass(WriteSequencesToCluster.Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

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
