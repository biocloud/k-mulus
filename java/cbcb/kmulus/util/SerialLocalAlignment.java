package cbcb.kmulus.util;

public class SerialLocalAlignment {	

	final static int SCORE_FACTOR = LocalAlignment.SCORE_FACTOR;
	private int score, len1, len2;

	public SerialLocalAlignment(int score, int len1, int len2) {
		this.score = score;
		this.len1 = len1;
		this.len2 = len2;
	}

	/**
	 * Aligns two sequences using the given values.
	 * 
	 * @param s1 First sequence.
	 * @param s2 Second sequence..
	 * @param m  Value awarded for matching two characters.
	 * @param mm  Value awarded for mismatching two characters.
	 * @param g  Value awarded for matching a character to a gap.
	 * @param e  Value awarded for gap extension.
	 * @return   The bit score alignment of the two sequences.
	 */
	public static SerialLocalAlignment align(String s1, String s2, int m, int mm, int g, int e) {

		/* Skip past the sequence ID. */
		int start1 = s1.indexOf(" ") + 1, start2 = s2.indexOf(" ") + 1;

		int len1 = s1.length() - start1, len2 = s2.length() - start2;

		/*Only two consecutive rows are needed at a time in memory for each of 4 tables:
		 * V is the best score thus far, G is the best score with a match at the end,
		 * E is the best score with a gap in s1, F is the best score with a gap in s2*/
		int[] lastRowV = new int[len2 + 1], lastRowF = new int[len2 + 1];
		int[] rowV = new int[len2 + 1], rowG = new int[len2 + 1], 
		rowE = new int[len2 + 1], rowF = new int[len2 + 1];

		int maxI = 0, maxJ = 0, max = 0;

		/* Main loop for determining the alignment. */
		for (int i = 1; i <= len1; i++) {

			/*Initialize first columns.*/
			rowV[0] =  rowG[0] = rowE[0] = rowF[0] = 0;

			for(int j = 1; j <= len2; j++) {

				rowG[j] = lastRowV[j-1] + (s1.charAt(start1 + i - 1) == s2.charAt(start2 + j-1) ? m : mm );
				rowE[j] = Math.max(rowE[j-1], rowV[j-1] + g) + e;
				rowF[j] = Math.max(lastRowF[j], lastRowV[j] + g) + e;

				if(rowG[j] >= rowE[j] && rowG[j] >= rowF[j]) {
					rowV[j] = rowG[j];

				} else if(rowE[j] >= rowG[j] && rowE[j] >= rowF[j]) {
					rowV[j] = rowE[j];

				} else {
					rowV[j] = rowF[j];

				}

				/*Track the maximum*/
				if(rowV[j] > max) {
					maxI = i;
					maxJ = j;
					max = rowV[j];
				}

				/*If the score is below zero, terminate the sequence*/
				if(rowV[j] <= 0) {
					rowV[j] = 0;
				}

			}

			/* Adjust reading frame down a row. */
			for(int k = 0; k < rowV.length; k++) {
				lastRowV[k] = rowV[k];
				lastRowF[k] = rowF[k];
			}

		}

		return new SerialLocalAlignment(max, len1, len2);
	}

	public int getScore() {
		return score;
	}

	public int getDistance() {
		return SCORE_FACTOR / (score + 1);
	}
}
