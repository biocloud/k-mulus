package cbcb.kmulus.blast;

import cbcb.kmulus.util.Biology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

/**
 * Mapper recieves a chunk of input simple fasta sequences. For each k-mer seen in
 * the query sequence, it is checked against each clusters k-mers vector.  If
 * the query shares a k-mer in common with a cluster, the (cluster_id, >query_seq)
 * is emitted.
 */
public class BlastMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private static final Logger LOG = Logger.getLogger(BlastMapper.class);

	private int alphabetSize;
	private int kmerLength;
	private int numberOfBytes;
	private int numCenters;
	
	// K-mer presence vector.
	private byte[][] clusterKmerVectors;

	/**
	 * Allocate space for the cluster center vectors.
	 */
	private void initializeCenters() {
		if (clusterKmerVectors != null)
			clusterKmerVectors = null;

		clusterKmerVectors = new byte[numCenters][numberOfBytes];
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
		String clusterDir = conf.get(Blast.CLUSTER_DIR);
		Path clusterPath = new Path(clusterDir);
		
		FileSystem fs = FileSystem.get(conf);
		
		int currCenter = 0;
		// Go through each part-r-* file in the clusterPath and
		// copy over their bit vectors.
		for (FileStatus srcFileStatus : fs.listStatus(clusterPath)) {
			if (srcFileStatus.isDir())
				continue;

			SequenceFile.Reader reader = new SequenceFile.Reader(fs,
					srcFileStatus.getPath(), conf);

			LongWritable key = new LongWritable();
			BytesWritable value = new BytesWritable();

			while (reader.next(key, value) != false) {
				if (key.toString().equals(""))
					break;

				System.arraycopy(value.getBytes(), 0,
						clusterKmerVectors[currCenter], 0,
						clusterKmerVectors[currCenter].length);
				
				++currCenter;
				key.set(0);
			}
			
			reader.close();
		}
		
		if (currCenter == numCenters) {
			return true;
		}
		else {
			LOG.info("Clusters expected: " + numCenters + ", actual clusters: " +
					currCenter);
			return false;
		}
	}
	
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		kmerLength = conf.getInt(Blast.KMER_LENGTH, 3);
		alphabetSize = conf.getInt(Blast.ALPHABET_SIZE, Biology.AMINO_ACIDS.length);
		numberOfBytes = (int) Math.ceil(Math.pow(alphabetSize, kmerLength) / 8);
		
		buildAminoAcidToIndexMap();
		
		try {
			loadClusters(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * TODO(cmhill): Generates a list of words above a given threshold value.
	 * 
	 * @param word
	 * @return List of words that are above a certain similarity.
	 */
	private List<String> getWordsAboveThreshold(String word) {
		List<String> wordList = new ArrayList<String>();
		wordList.add(word);
		return wordList;
	}

	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String headerAndSequence = value.toString().trim();
		
		// TODO(cmhill): More error checking.
		if (!headerAndSequence.startsWith(">")) {
			LOG.info("INPUT SEQUENCE ERROR: " + headerAndSequence);
			return;
		}
		
		String sequence = headerAndSequence.split(Blast.HEADER_SEQUENCE_SEPARATOR)[1];
		
		// Go through all k-mers of the sequence
		Set<Integer> clusters = new HashSet<Integer>();
		Set<Integer> overlappingClusters = new HashSet<Integer>();
		
		// Faster way than this?
		for (int i = 0; i < numCenters; i++)
			clusters.add(i);
		
		for (int i = 0; i <= sequence.length() - kmerLength; i += kmerLength) {
			// TODO(cmhill): Get all words above the given threshold.
			List<String> words = getWordsAboveThreshold(sequence.substring(i, i + kmerLength));
			
			for (String word : words) {
				
				// Cycle through each cluster and if the kmer/word appears in
				// the cluster, add the cluster to the overlappingClusters set.
				int wordIndex = getIndexForSequence(word);
				int byteOffset = wordIndex / 8;
				int bitOffset = wordIndex % 8;
				int mask = 1 << bitOffset;
				
				//for (int j = 0; j < clusterKmerVectors.length; j++) {
				for (Integer j : clusters) {
					// Is this position set in the cluster vector?
					if ((clusterKmerVectors[j][byteOffset] & mask) != 0) {
						overlappingClusters.add(j);
					}
				}
				
				// Remove all clusters we've already found overlaps for.
				clusters.removeAll(overlappingClusters);
			}
		}
		
		// Emit all overlapping clusters.
		for (Integer clusterId : overlappingClusters) {
			context.write(new LongWritable(clusterId), value);
		}
		
		// Toy example, just send the sequence to the clusterID.
		/*String[] clusterAndSequence = value.toString().trim().split("\t");
		
		context.write(new LongWritable(new Long(clusterAndSequence[0])),
				new Text(clusterAndSequence[1]));*/
	}
	
	/*
	 * These functions should be a utility bit vector class 
	 */
	private Map<Character, Integer> aaToIndex;
	
	private void buildAminoAcidToIndexMap() {
		int index = 0;
		aaToIndex = new HashMap<Character, Integer>();
		for (Character aa : Biology.AMINO_ACIDS) {
			aaToIndex.put(aa, index);
			index++;
		}
	}
	
	public int getIndexForSequence(String sequence) {
		char aminoAcid = 'X';
		int power = this.kmerLength - 1;
		int index = 0;
		
		for (int i = 0; i < sequence.length(); i++) {
			aminoAcid = sequence.charAt(i);
			if (aaToIndex.get(aminoAcid) == null) {
				System.out.println("aa: " + aminoAcid);
			} else {
				index += (aaToIndex.get(aminoAcid) * Math.pow(this.alphabetSize, power));
				power--;
			}
		}
		
		return index;
	}
}
