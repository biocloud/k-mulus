package cbcb.kmulus.metrics;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import cbcb.kmulus.util.Biology;
import cbcb.kmulus.util.PresenceVector;

/**
 * Counts the number of clusters mapped to each sequence. Emits (seq_header, count).
 */
public class QueryCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private static final Logger LOG = Logger.getLogger(CountQueryMaps.class);

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
		String clusterDir = conf.get(CountQueryMaps.CLUSTER_DIR);
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
		kmerLength = conf.getInt(CountQueryMaps.KMER_LENGTH, 3);
		numCenters = conf.getInt(CountQueryMaps.NUM_CENTERS, -1);
		
		try {
			loadClusters(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String headerAndSequence = value.toString().trim();
		
		// TODO(cmhill): More error checking.
		if (!headerAndSequence.startsWith(">")) {
			LOG.info("INPUT SEQUENCE ERROR: " + headerAndSequence);
			return;
		}
		
		String[] headerSeq = headerAndSequence.split(CountQueryMaps.HEADER_SEQUENCE_SEPARATOR);
				
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