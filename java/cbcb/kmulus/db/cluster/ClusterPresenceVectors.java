package cbcb.kmulus.db.cluster;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;

import cbcb.kmulus.util.Biology;
import cbcb.kmulus.util.PresenceVector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Hadoop program that clusters a HDFS directory of {@link PresenceVector}.
 */
public class ClusterPresenceVectors extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(ClusterPresenceVectors.class);
	private static final String USAGE = "ClusterPresenceVectors KMER_VECTOR_INPUT OUTPUT NUM_SEQUENCES NUM_CLUSTERS [NUM_TASKS]";
	
	public static final String FINAL_DIR = "final";
	
	protected static final String DEBUG = "DEBUG";
	protected static final String INPUT_PATH = "INPUT_PATH";
	protected static final String ITERATION = "ITERATION";
	protected static final String LOG_DELIM = ",";
	protected static final String KMER_LENGTH = "KMER_LENGTH";
	protected static final String NUM_CLUSTERS = "NUM_CLUSTERS";
	
	protected static final int DEFAULT_KMER_LENGTH = 3;
	
	private static final int MAX_REDUCES = 100;
	private static final int MAX_MAPS = 100;
	private static final int MAX_ITERATIONS = 5;
	
	private static final int LARGE_PRIME1 = 27277;
	private static final int LARGE_PRIME2 = 30707;
	
	/* Runtime return codes. */
	public static final int CODE_ERROR = -1;
	public static final int CODE_LOOP = 0;
	public static final int CODE_FINISHED = 1;
	public static final int CODE_CONVERGED = 2;
	
	protected final int runIter;
	protected final boolean finished;
	
	public ClusterPresenceVectors() {
		runIter = 0;
		finished = true;
	}
	
	public ClusterPresenceVectors(int runIter) {
		this.runIter = runIter;
		finished = false;
	}
	
	/**
	 * This mapper takes as input a {@link PresenceVector} and emits (id
	 * of the closest cluster, {@link PresenceVector}).
	 */
	public static class Map extends Mapper<LongWritable, PresenceVector, LongWritable, PresenceVector> {
		
		private boolean debug = false;
		
		private int kmerLength;
		private int numCenters;
		private int alphabetSize;
		
		private PresenceVector[] centers;
		
		/**
		 * Load the cluster {@link PresenceVector} into the memory.
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			
			int iteration = new Integer(context.getConfiguration().getInt(ITERATION, -1));
			
			kmerLength = context.getConfiguration().getInt(KMER_LENGTH, 3);
			numCenters = context.getConfiguration().getInt(NUM_CLUSTERS, -1);
			debug = context.getConfiguration().getBoolean(DEBUG, false);
			
			if (numCenters < 0) {
				throw new IOException("The number centers was not given in the configuration.");
			}
			
			String input = context.getConfiguration().get(INPUT_PATH);
			LOG.info("Loading cluster centers from: " + input + "/output-" + iteration);
			
			initializeCenters();
			
			Path centersPath = new Path(input + "/output-" + iteration);
			
			int currCenter = 0;
			// Go through each part-r-* file in the centersPath and add the centers to the TreeSet.
			for (FileStatus srcFileStatus : fs.listStatus(centersPath)) {
				if (srcFileStatus.isDir())
					continue;

				SequenceFile.Reader reader = new SequenceFile.Reader(fs, srcFileStatus.getPath(), conf);
			
				LongWritable key = new LongWritable();
				PresenceVector value = new PresenceVector();
				
				while (reader.next(key, value) != false) {
					if (key.toString().equals(""))
						break;

					centers[currCenter] = new PresenceVector(value);
					++currCenter;
					
					key.set(0);
				}
				
				reader.close();
			}
			
			// The number of centers may have been reduced after previous iterations.
			numCenters = currCenter;
			
			if (debug) {
				LOG.info("Number of centers loaded: " + currCenter);
			}
		}

		/**
		 * Initialize the array {@link PresenceVector}. 
		 */
		private void initializeCenters() {
			if (centers != null)
				centers = null;
						
			centers = new PresenceVector[numCenters];
		}
		
		public void map(LongWritable key, PresenceVector value, Context context) 
				throws IOException, InterruptedException {
			if (centers == null) {
				throw new IOException("Centers are uninitialized.");
			}
			 
			// Go through each center and compute minimum distance.
			Long closestCenter = new Long(-1);
			int minDistance = Integer.MAX_VALUE;
			
			// Store the closest centers.
			ArrayList<Long> closestCenters = new ArrayList<Long>();
			
			for (int i = 0; i < numCenters; i++) {
				if (centers[i] == null) {
					throw new IOException("Center '" + i + "' was uninitialized.");
				}
				int distance = centers[i].getHammingDistance(value);
				
				// If the current sequence is the same as the center, MUST map to it.
				if (centers[i].getId() == value.getId() && centers[i].getId() >= 0) {
					closestCenters.clear();
					closestCenters.add(new Long(i));
					break;
					
			    } else if (distance < minDistance) {
					closestCenter = new Long(i);
					minDistance = distance;
					closestCenters.clear();
					closestCenters.add(closestCenter);
				} else if (distance == minDistance) {
					closestCenters.add(new Long(i));
				}
			}
			
			// Randomly select a center.
			int item = new Random().nextInt(closestCenters.size());		
			closestCenter = closestCenters.get(item);
			
			if (debug) {
				// TODO(cmhill) log PV specs
			}
			
			if (closestCenter == -1) {
				throw new IOException("No nearest center found for sequence: " + key.get());
				
			} else {
				context.write(new LongWritable(closestCenter), value);
			}
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path(args[1] + "/temp"), true);
		FileSystem.get(conf).delete(new Path(args[1]), true);
		
		int res = 0;
		int runIter = 0;
		do {
			res = ToolRunner.run(new Configuration(), new ClusterPresenceVectors(runIter), args);
			runIter++;
		} while (res == 0);
		
		if (res == CODE_CONVERGED) {
			res = ToolRunner.run(new Configuration(), new ClusterPresenceVectors(), args);
		}
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 4) {
			System.out.println(USAGE);
			return CODE_ERROR;
		}

		String sequenceInputPath = args[0];
		String tempInput = args[1] + "/temp";
		String outputPath = args[1];
		String numSequences = args[2];
		String numClusters = args[3];
		
		LOG.info("Tool name: ClusterPresenceVectors");
		LOG.info(" - sequencePresenceVectors: " + sequenceInputPath);
		LOG.info(" - outputDir: " + outputPath);
		LOG.info(" - numSequences: " + numSequences);
		LOG.info(" - numClusters: " + numClusters);

		Job job = new Job(getConf(), "ClusterPresenceVectors");
		job.setJarByClass(ClusterPresenceVectors.class);
		
		Configuration conf = job.getConfiguration();
		conf.setInt(ITERATION, runIter);
		conf.set(INPUT_PATH, tempInput);
		conf.setInt(KMER_LENGTH, 3);
		conf.setBoolean(DEBUG, true);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PresenceVector.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PresenceVector.class);

		job.setMapperClass(ClusterPresenceVectors.Map.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(sequenceInputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		int mapTasks = MAX_MAPS;
		int reduceTasks = MAX_REDUCES;

		if (args.length > 4) {
			
			conf.setInt(KMER_LENGTH, Integer.parseInt(args[4]));
			
			if (args.length > 5) {
				int numTasks = Integer.parseInt(args[5]);
				mapTasks = numTasks;
				reduceTasks = numTasks;
				
				if (args.length > 6) {
					conf.setBoolean(DEBUG, true);
				}
			}
		}
		
		int maxSequenceNumber = Integer.parseInt(numSequences);
		int clusters = Integer.parseInt(numClusters);
		conf.setInt(NUM_CLUSTERS, clusters);
		
		/* Setup the key value pairs */
		job.setNumReduceTasks(reduceTasks);

		if (finished || runIter - 2 > MAX_ITERATIONS) {
			
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(PresenceVector.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(PresenceVector.class);

			job.setMapperClass(ClusterPresenceVectors.Map.class);
			FileOutputFormat.setOutputPath(job, new Path(outputPath + Path.SEPARATOR + FINAL_DIR));
			
			job.waitForCompletion(true);		
			return CODE_FINISHED;

		} else {
			job.setReducerClass(KMeansReducer.class);
			FileOutputFormat.setOutputPath(job, new Path(tempInput + "/output-"
					+ (runIter + 1)));
		}
		
		FileSystem fs = FileSystem.get(conf);

		if (runIter == 0) {
			int index = new Random().nextInt();
			int largePrime = maxSequenceNumber == LARGE_PRIME1 ? LARGE_PRIME2 : LARGE_PRIME1;
			TreeSet<Integer> t = new TreeSet<Integer>();
			
			// Adding a large prime will eventually visit all sequences without repeating.
			while (t.size() < clusters) {
				t.add(index);
				index = (index + largePrime) % maxSequenceNumber;
			}
				
			fs = FileSystem.get(conf);
			
			int currPart = 0;
			
			NumberFormat formatter = new DecimalFormat("00000");
			
			// Write out the centers presencevectors as the first iteration.
			SequenceFile.Writer sf = new SequenceFile.Writer(fs, conf,
					new Path(tempInput + "/output-0/part-r-00000"),
					LongWritable.class, PresenceVector.class);
			

			// Bootstrap with the first k sequences from input.
			SequenceFile.Reader reader = new SequenceFile.Reader(fs,
					new Path(sequenceInputPath + "/part-r-"
					+ formatter.format(currPart)), conf);

			LongWritable key = new LongWritable();
			PresenceVector value = new PresenceVector();

			LOG.info("Creating file at: " + tempInput
					+ "/output-0/part-r-00000");

			for (int i = 0; i < new Integer(numClusters); i++) {
				if (reader.next(key, value) != true) {
					LOG.info("Stopped at clusters: " + i);
					
					++currPart;
					reader = new SequenceFile.Reader(fs, new Path(sequenceInputPath
							+ "/part-r-" + formatter.format(currPart)), conf);
				}
				
				sf.append(key, value);				
			}
			
			sf.close();
		}
		
		job.setNumReduceTasks(reduceTasks);
		long startTime = System.currentTimeMillis();
		boolean result = job.waitForCompletion(true);

		LOG.info((System.currentTimeMillis() - startTime) + LOG_DELIM
				+ mapTasks + LOG_DELIM + reduceTasks);

		return result ? CODE_LOOP : CODE_ERROR;
	}
}
