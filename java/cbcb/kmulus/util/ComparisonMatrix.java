package cbcb.kmulus.util;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/** A comparison matrix for amino acid similarity. */
public class ComparisonMatrix {
	                    
	private int[][] matrix = new int[Character.MAX_VALUE][];

	/**
	 * Initializes this comparison matrix; comparison scores between all pairs of amino acids must
	 * be provided.
	 * 
	 * @param scores a mapping of each pair of amino acid characters to their corresponding score
	 */
	public ComparisonMatrix(Map<Character, Map<Character, Integer>> scores) {
		
		/* Ensure all amino acid residues are included in the given map. */
		String missingAAs = "";
		for (Character aa : Biology.AMINO_ACIDS) {
			char upperAA = aa.toString().toUpperCase().charAt(0);
			
			if (!scores.containsKey(upperAA) && !scores.containsKey(aa)) {
				missingAAs += aa + ",";
			}
		}
		
		if (!missingAAs.equals("")) {
			throw new RuntimeException("Failed to intialize the ComparisonMatrix, "
					+ "amino acid codes: " + missingAAs + " were left undefined in the input.");
		}
		
		Map<Character, Integer> aaCount = Maps.newHashMap();
		for (char aa : Biology.AMINO_ACIDS) {
			aaCount.put(aa, 0);
		}
		
		for (Character c : scores.keySet()) {
			if (matrix[c] != null) {
				continue;
			}
			
			char upperC = c.toString().toUpperCase().charAt(0);
			char lowerC = c.toString().toLowerCase().charAt(0);
			matrix[upperC] = matrix[lowerC] = new int[Character.MAX_VALUE];
			
			for (Character o : scores.get(c).keySet()) {
				char upperO = o.toString().toUpperCase().charAt(0);
				char lowerO = o.toString().toLowerCase().charAt(0);
				matrix[c][upperO] = matrix[c][lowerO] = scores.get(c).get(o);
				
				if (matrix[o] == null) {
					matrix[upperO] = matrix[lowerO] = new int[Character.MAX_VALUE];
				}
				matrix[o][upperC] = matrix[o][lowerC] = matrix[c][o];
				
				aaCount.put(lowerO, aaCount.get(lowerO) + 1);
			}
			aaCount.put(lowerC, aaCount.get(lowerC) + scores.get(c).size());
		}
		
		String missingComparisons = "";
		for (char aa : aaCount.keySet()) {
			if (aaCount.get(aa) != Biology.AMINO_ACIDS.length) {
				missingComparisons += aa + ",";
			}
		}
		
		if (!missingComparisons.equals("")) {
			throw new RuntimeException("Failed to initialize the ComparisonMatrix, amino acids: " + 
					missingComparisons + " were missing comparisons.");
		}
		
		// TODO(CH): Support the weird cases (X,...).
	}

	/** Return the score for this {@link ComparisonMatrix} of aligning the given amino acids. */
	public int getScore(char aa1, char aa2) {
		Preconditions.checkNotNull(matrix[aa1]);
		return matrix[aa1][aa2];
	}
}
