package cbcb.kmulus.db.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cbcb.kmulus.util.Biology;
import cbcb.kmulus.util.PresenceVector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

/**
 * Hadoop program that runs the K-Medoid update algorithm.  Given a list of 
 * {@link PresenceVector}, emit a {@link PresenceVector} where each bit position
 * is the average of that position across all {@link PresenceVector} passed in.
 */
public class KMedoidReducer extends Reducer<LongWritable, PresenceVector, LongWritable, PresenceVector> {

	private boolean debug = false;
	private static final Logger LOG = Logger.getLogger(KMedoidReducer.class);
	
	private int kmerLength;
	private int numCenters;

	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		kmerLength = conf.getInt(ClusterPresenceVectors.KMER_LENGTH, 3);
		numCenters = conf.getInt(ClusterPresenceVectors.NUM_CLUSTERS, 10);
		debug = conf.getBoolean(ClusterPresenceVectors.DEBUG, false);
	}
		
	public void reduce(LongWritable key, Iterable<PresenceVector> values, Context context)
			throws IOException, InterruptedException {

		if (debug)
			LOG.info("RedKey: " + key.toString());
		
		int numOfSequences = 0;
		List<PresenceVector> vectors = new ArrayList<PresenceVector>();
		
		// copy all values
		for (PresenceVector value : values) {			
			vectors.add(new PresenceVector(value));
			++numOfSequences;			
		}

		if (debug) {
			LOG.info("Number of sequences: " + numOfSequences);
		}
		
		PresenceVector clusterCenter = null;
		int minDistance = Integer.MAX_VALUE;
		for (int i = 0; i < vectors.size(); i++) {
			PresenceVector tempCenter = vectors.get(i);
			int distance = 0;
			
			for(int j=0; j < vectors.size(); j++){
				distance+= tempCenter.getHammingDistance(vectors.get(j));
			}
			
			if (distance < minDistance){
				clusterCenter = tempCenter;
				minDistance = distance;
			}
		}
		
		context.write(key, clusterCenter);
	}
}
