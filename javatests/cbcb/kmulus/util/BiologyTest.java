package cbcb.kmulus.util;

import junit.framework.TestCase;

/** Tests for {@link Biology}. */
public class BiologyTest extends TestCase {

	/** Test for {@link Biology#TRIPLETS}. */
	public void testTriplets() {		
		for (char a : Biology.NUCLEOTIDES) {
			for (char b : Biology.NUCLEOTIDES) {
				for (char c : Biology.NUCLEOTIDES) {
					String triplet = "" + a + b + c;
					
					boolean foundTriplet = false;
					for (String actualTriplet : Biology.TRIPLETS) {
						if (triplet.equals(actualTriplet)) {
							assertFalse("Triplet '" + triplet + " appeared multiple times.",
									foundTriplet);
							foundTriplet = true;
						}
					}
					assertTrue("Triplet '" + triplet + "was not present.", foundTriplet);
				}
			}
		}
	}
	
	/** Test for {@link Biology#defaultTable}. */
	public void testDefaultTable() {
		for (String triplet : Biology.TRIPLETS) {
			char aa = Biology.defaultTable.get(triplet);
			if (aa != Biology.TERMINATOR) {
				
				boolean foundAA = false;
				for (char actualAA : Biology.AMINO_ACIDS) {
					if (aa == actualAA) {
						foundAA = true;
						break;
					}
				}
				assertTrue("Triplet '" + triplet + "' pointed to amino acid '" + aa + 
						"' in the default table.", foundAA);
			}
		}
	}
}
