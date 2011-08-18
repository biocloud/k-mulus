package cbcb.kmulus.allpairs.protein;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import cbcb.kmulus.util.Alignment;

import com.google.common.base.Preconditions;

/** TODO(calbach): Justify this class's existence. */
public class KmerDistance implements Alignment {	
	
	private static final double SCALING_FACTOR = Integer.MAX_VALUE;
	private static final double MAX_DISTANCE = Math.log(1.1);
	
	private final long distance;
	
	private KmerDistance(long distance) {
		this.distance = distance;
	}
	
	public long getDistance() {
		return distance;
	}
	
	public static KmerDistance getKmerAlignment(Text a, Text b, int kmerLength) {
		return new KmerDistance(getKmerDistance(a, b, kmerLength));
	}
	
	public static long getKmerDistance(Text a, Text b, int kmerLength) {
		Preconditions.checkNotNull(a);
		Preconditions.checkNotNull(b);
		Preconditions.checkArgument(kmerLength > 0);
		
		String aStr = a.toString();
		String bStr = b.toString();
	
		// Build a kmer count hashmap for the two sequences.
		Map<String, Integer> kmerCountA = buildKmerMap(aStr, kmerLength);
		Map<String, Integer> kmerCountB = buildKmerMap(bStr, kmerLength);
		
		// Compute the kmer occurence frequency.
		double summation = 0.0;
		for (String kmer : kmerCountA.keySet()) {
			if (kmerCountB.get(kmer) != null) {
				summation += Math.min(kmerCountA.get(kmer), kmerCountB.get(kmer));
			}
		}
	
		int numKmers = Math.min(aStr.length(), bStr.length()) - kmerLength + 1;
		double kmerDistance =
			MAX_DISTANCE - Math.log(.1 + (numKmers <= 0 ? 0 : (summation / numKmers)));

		// Normalize it to LongWritable.
		return (long) (SCALING_FACTOR * kmerDistance);
	}

	/**
	 * Builds and returns a k-mer map for the given seq.
	 */
	public static Map<String, Integer> buildKmerMap(String seq, int kmerLength) {
		Map<String, Integer> kmerCount = new HashMap<String, Integer>();
		for (int i = 0; i <= seq.length() - kmerLength; i++) {
			String kmer = seq.substring(i, i + kmerLength);
			
			if (kmerCount.containsKey(kmer)) {
				kmerCount.put(kmer, kmerCount.get(kmer) + 1);
			} else {
				kmerCount.put(kmer, 1);
			}
		}

		return kmerCount;
	}
}
