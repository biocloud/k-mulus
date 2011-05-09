package cbcb.kmulus.db.processing;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class WriteClusterSequencesToHDFS extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(WriteClusterSequencesToHDFS.class);
	
	private static final String USAGE = "WriteClusterSequencesToHDFS CLUSTER_INPUT SEQUENCE_INPUT BASE_OUTPUT_DIR OUTPUT_DIR NUM_CLUSTERS [NUM_TASKS]";
	
	public static final String LOG_DELIM = ",";
	public static final String SIMPLE_FASTA_SPLIT = " ";
	public static final String CLUSTER_SEPARATOR = "\t";
	public static final String BASE_OUTPUT_DIR = "BASE_OUTPUT_DIR";
	public static final String NUM_CLUSTERS = "NUM_CLUSTERS";
	
	private static final int MAX_REDUCES = 200;
	private static final int MAX_MAPS = 200;

	
	public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text>   {
		private FileSystem hdfs;
		private String baseOutputDir;
		private boolean failedSetup = false;
		private int numberOfClusters;


		private int unique_id;
		
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			
			baseOutputDir = conf.get(BASE_OUTPUT_DIR);
			numberOfClusters = conf.getInt(NUM_CLUSTERS, 1);
			
			// TODO(cmhill): Better way to get a "unique" number for the reducer.
			Random rand = new Random();
			unique_id = rand.nextInt(10000000);
			
			try {
				hdfs = FileSystem.get(conf);
			} catch (IOException e) {
				failedSetup = true;
			}
		}
		
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// Output the sequence in fasta format to the correct hdfs directory.
			BufferedWriter bw = getBufferedWriterForCluster(key.get());

			String line = null;
			for (Text value : values) {
				line = value.toString().trim();
				String headerAndSequence[] = line.split(SIMPLE_FASTA_SPLIT); 
				bw.write(headerAndSequence[0] + "\n" + headerAndSequence[1] + "\n");
			}
			bw.close();
		}
		
		/**
		 * Return the BufferedWriter for the hdfs file of the cluster id.
		 * 
		 * @param clusterId
		 * @return BufferedWriter for the hdfs file "[BASE_OUTPUT_DIR]/[CLUSTER_ID]/seq.[RANDOM_NUMBER]"
		 * @throws IOException
		 */
		private BufferedWriter getBufferedWriterForCluster(long clusterId) throws IOException {
			FSDataOutputStream outputStream = hdfs.create(new Path(baseOutputDir + "/" + clusterId + "/seq." + unique_id));
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
			return writer;
		}
	}
	
	public static void main(String[] args) {
		int result = 1;

		try {
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
		
		if(args.length < 3) {
			System.out.println(USAGE);
			return -1;
		}
		
		String sequenceInputPath = args[0];
		String baseOutputPath = args[1];
		String outputDir = args[2];
		int numberOfClusters = new Integer(args[3]);
		
		LOG.info("Tool name: WriteClusterSequencesToHDFS");
		LOG.info(" - sequenceInputDir: " + sequenceInputPath);
		LOG.info(" - baseOutputPath: " + baseOutputPath);
		LOG.info(" - numberOfClusters: " + numberOfClusters);
		
		Job job = new Job(getConf(), "WriteClusterSequencesToHDFS");
		job.setJarByClass(WriteClusterSequencesToHDFS.class);

		job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048M");
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(WriteClusterSequencesToHDFS.Reduce.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// FileInputFormat.addInputPath(job, new Path(clusterInputPath));
		FileInputFormat.addInputPath(job, new Path(sequenceInputPath));
		
		// Useless output directory.
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		int mapTasks = MAX_MAPS;
		int reduceTasks = MAX_REDUCES;

		if(args.length > 4) {
			int numTasks = Integer.parseInt(args[4]);
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
