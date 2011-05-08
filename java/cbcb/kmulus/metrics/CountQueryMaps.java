package cbcb.kmulus.metrics;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cbcb.kmulus.blast.Blast;
import cbcb.kmulus.blast.BlastMapper;
import cbcb.kmulus.util.Biology;
import cbcb.kmulus.util.PresenceVector;

/** 
 * Hadoop program which counts the number of sequences which are mapped to each cluster. Should only
 * be run after database clustering has been completed.
 * 
 * Emits values of the form (cluster_id, count), where count is the number of sequences which map
 * to that cluster center.
 * 
 * @see Blast
 */
public class CountQueryMaps extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(CountQueryMaps.class);
	private static final String USAGE = "Blast SEQUENCE_INPUT CLUSTER_INPUT OUTPUT [NUM_TASKS]";
	
	protected static final String ALPHABET_SIZE = "ALPHABET_SIZE";
	protected static final String BLAST_DATABASES = "BLAST_DATABASES";
	protected static final String CLUSTER_DIR = "CLUSTER_DIR";
	protected static final String HEADER_SEQUENCE_SEPARATOR = " ";
	protected static final String KMER_LENGTH = "KMER_LENGTH";
	
	private static final int MAX_REDUCES = 200;
	private static final int MAX_MAPS = 200;

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

		if (args.length < 3) {
			System.out.println(USAGE);
			return -1;
		}

		String sequenceInputPath = args[0];
		String clusterInputPath = args[1];
		String outputPath = args[2];

		LOG.info("Tool name: " + CountQueryMaps.class.getName());
		LOG.info(" - sequenceInputDir: " + sequenceInputPath);
		LOG.info(" - clusterInputDir: " + clusterInputPath);
		LOG.info(" - outputDir: " + outputPath);
		
		Job job = new Job(getConf(), CountQueryMaps.class.getName());
		job.setJarByClass(CountQueryMaps.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setMapperClass(BlastMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(sequenceInputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		int mapTasks = MAX_MAPS;
		int reduceTasks = MAX_REDUCES;

		if(args.length > 3) {
			int numTasks = Integer.parseInt(args[3]);
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
	
	/**
	 * Counts the number of clusters mapped to each sequence. Emits (seq_header, count).
	 */
	public class QueryCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private int alphabetSize;
		private int kmerLength;
		private int numCenters;
		
		/** The {@link PresenceVector} for each given cluster center. */
		private PresenceVector[] clusterCenters;
	 
		/** Allocate space for the cluster center vectors. */
		private void initializeCenters() {
			if (clusterCenters != null) {
				clusterCenters = null;
			}

			clusterCenters = new PresenceVector[numCenters];
		}

		/**
		 * Load the cluster bit vectors in from the specified HDFS path.
		 * 
		 * @param clusterDir HDFS directory of cluster center bit vectors.
		 * @return True if centers copied succesfully, false otherwise.
		 * @throws IOException  If hdfs path related error.
		 */
		private boolean loadClusters(Configuration conf) throws IOException {
			initializeCenters();
			
			// Load each cluster k-mer bit vectors into memory.
			String clusterDir = conf.get(CLUSTER_DIR);
			Path clusterPath = new Path(clusterDir);
			
			FileSystem fs = FileSystem.get(conf);
			
			int currCenter = 0;
			// Go through each part-r-* file in the clusterPath and
			// copy over their bit vectors.
			for (FileStatus srcFileStatus : fs.listStatus(clusterPath)) {
				if (srcFileStatus.isDir())
					continue;

				SequenceFile.Reader reader = new SequenceFile.Reader(fs,srcFileStatus.getPath(), conf);

				LongWritable key = new LongWritable();
				PresenceVector value = new PresenceVector();

				while (reader.next(key, value) != false) {
					if (key.toString().equals("")) {
						break;
					}

					clusterCenters[currCenter] = new PresenceVector(value);
					++currCenter;
					key.set(0);
				}
				reader.close();
			}
			
			if (currCenter == numCenters) {
				return true;
				
			} else {
				LOG.info("Clusters expected: " + numCenters + ", actual clusters: " +
						currCenter);
				return false;
			}
		}
		
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kmerLength = conf.getInt(KMER_LENGTH, 3);
			alphabetSize = conf.getInt(ALPHABET_SIZE, Biology.AMINO_ACIDS.length);
			
			try {
				loadClusters(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String headerAndSequence = value.toString().trim();
			
			// TODO(cmhill): More error checking.
			if (!headerAndSequence.startsWith(">")) {
				LOG.info("INPUT SEQUENCE ERROR: " + headerAndSequence);
				return;
			}
			
			String[] headerSeq = headerAndSequence.split(HEADER_SEQUENCE_SEPARATOR);
			String header = headerSeq[0];
			String sequence = headerSeq[1];
			
			// Go through all k-mers of the sequence
			Set<Integer> clusters = new HashSet<Integer>();
			Set<Integer> overlappingClusters = new HashSet<Integer>();
			
			// Faster way than this?
			for (int i = 0; i < numCenters; i++) {
				clusters.add(i);
			}
			
			for (int i = 0; i < sequence.length() - kmerLength; i++) {
				
				String kmer = sequence.substring(i, i + kmerLength);
				Set<Integer> kmerHashes = Biology.getNeighborKmerHashes(kmer, 0);
				for (int kmerHash : kmerHashes) {

					// Check which cluster centers overlap with this k-mer.
					for (Integer j : clusters) {		
						if (clusterCenters[j].containsKmer(kmerHash)) {
							overlappingClusters.add(j);
						}
					}
					
					// Remove all clusters we've already found overlaps for.
					clusters.removeAll(overlappingClusters);
				}
			}
			
			// Emit the number of overlapping clusters for this sequence.
			context.write(new Text(header), new LongWritable(overlappingClusters.size()));
		}
	}
}
