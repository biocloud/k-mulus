package cbcb.kmulus.db.cluster;

import java.io.IOException;
import java.util.Arrays;

import cbcb.kmulus.util.Biology;
import cbcb.kmulus.util.PresenceVector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

/**
 * Hadoop program that runs the K-Means update algorithm.  Given a list of 
 * {@link PresenceVector}, emit a {@link PresenceVector} where each bit position
 * is the average of that position across all {@link PresenceVector} passed in.
 */
public class KMeansReducer extends Reducer<LongWritable, PresenceVector, LongWritable, PresenceVector> {

	private boolean debug = false;
	private static final Logger LOG = Logger.getLogger(KMeansReducer.class);
	
	private int kmerLength;

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		kmerLength = conf.getInt(ClusterPresenceVectors.KMER_LENGTH, 3);
		debug = conf.getBoolean(ClusterPresenceVectors.DEBUG, false);
	}
		
	public void reduce(LongWritable key, Iterable<PresenceVector> values, Context context)
			throws IOException, InterruptedException {

		if (debug)
			LOG.info("RedKey: " + key.toString());
		
		Double[] distances = new Double[(int) Math.ceil(Math.pow(
				Biology.AMINO_ACIDS.length, kmerLength))];
		Arrays.fill(distances, 0.0);
		
		int numOfSequences = 0;
		
		// Create the feature vector.
		for (PresenceVector value : values) {
			
			for (int position : value.getAllPresentHashes()) {
				distances[position] += 1;
			}
			
			if (debug) {
				// LOG.info(pv.printByteVector());
				// LOG.info(pv.printKmers());
			}
					
			++numOfSequences;			
		}

		if (debug) {
			LOG.info("Distances: " + distances.toString());
			LOG.info("Number of sequences: " + numOfSequences);
		}
		
		PresenceVector clusterCenter = new PresenceVector(kmerLength);
		
		// Normalize the presence vector.
		for (int i = 0; i < distances.length; i++) {
			// Distance metric relied on bit vectors of only 0 or 1, not counts.
			int distance = (int) Math.round(distances[i] / ((double) numOfSequences));
			if (distance > 0)
				clusterCenter.setKmer(i);
		}
		
		context.write(key, clusterCenter);
	}
}