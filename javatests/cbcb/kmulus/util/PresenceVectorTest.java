package cbcb.kmulus.util;

import cbcb.kmulus.util.PresenceVector;
import cbcb.kmulus.util.Biology;

import junit.framework.TestCase;

/** Tests for {@link PresenceVector}. */
public class PresenceVectorTest extends TestCase {
	protected int kmerLength = 2;
	protected char[] residues = {'A', 'B', 'C'};
	
	/** Test for {@link PresenceVector#PresenceVector(int, char[])} */
	public void testPresenceVector() {
		PresenceVector pv = new PresenceVector(kmerLength, residues.length);
		assertTrue(pv != null);
	}
	
	/** Test for {@link PresenceVector#setKmer(int)} */
	public void testSetKmer() {
		PresenceVector pv = new PresenceVector(kmerLength, residues.length);
	
		pv.setKmer(0);
		assertTrue(pv.containsKmer(0));
		assertFalse(pv.containsKmer(1));
		
		pv.setKmer(1);
		assertTrue(pv.containsKmer(1));
	}
	
	/** Test for {@link PresenceVector#setKmer(int)} */
	public void testSetKmerBiology() {
		PresenceVector pv = new PresenceVector(kmerLength, Biology.AMINO_ACIDS.length);
	
		pv.setKmer(Biology.getAAKmerHash("AA"));
		assertTrue(pv.containsKmer(Biology.getAAKmerHash("AA")));
		assertFalse(pv.containsKmer(Biology.getAAKmerHash("AV")));
		
		pv.setKmer(Biology.getAAKmerHash("VV"));
		pv.setKmer(Biology.getAAKmerHash("AA"), false);

		assertTrue(pv.containsKmer(Biology.getAAKmerHash("VV")));
		assertFalse(pv.containsKmer(Biology.getAAKmerHash("AA")));
	}
	
	/** Test for {@link PresenceVector#PresenceVector(PresenceVector)} */
	public void testCopyConstructor() {
		PresenceVector pv = new PresenceVector(kmerLength, residues.length);
		pv.setKmer(0); pv.setKmer(1); pv.setKmer(4);
		
		PresenceVector otherPv = new PresenceVector(pv);
		pv.setKmer(0, false);
		
		assertTrue(otherPv.containsKmer(0) && otherPv.containsKmer(1) &&
				otherPv.containsKmer(4));
	}
	
	/** Test for {@link PresenceVector#intersect(PresenceVector)} */
	public void testIntersect() {
		PresenceVector pv = new PresenceVector(kmerLength, residues.length);
		pv.setKmer(2); pv.setKmer(1); pv.setKmer(4);
		
		PresenceVector otherPv = new PresenceVector(kmerLength, residues.length);
		otherPv.setKmer(3); otherPv.setKmer(5); otherPv.setKmer(4);
		
		PresenceVector intersectPv = otherPv.intersect(pv);
		assertTrue(intersectPv.containsKmer(4) && !intersectPv.containsKmer(2) &&
				!intersectPv.containsKmer(5));
	}
	
	/** Test for {@link PresenceVector#intersectEquals(PresenceVector)} */
	public void testIntersectEquals() {
		PresenceVector pv = new PresenceVector(kmerLength, residues.length);
		pv.setKmer(2); pv.setKmer(1); pv.setKmer(4);
		
		PresenceVector otherPv = new PresenceVector(kmerLength, residues.length);
		otherPv.setKmer(3); otherPv.setKmer(5); otherPv.setKmer(4);
		
		PresenceVector intersectPv = otherPv.intersectEquals(pv);
		assertTrue(intersectPv.containsKmer(4) && !intersectPv.containsKmer(3));
	}
	
	/** Test for {@link PresenceVector#union(PresenceVector)} */
	public void testUnion() {
		PresenceVector pv = new PresenceVector(kmerLength, residues.length);
		pv.setKmer(2); pv.setKmer(1);
		
		PresenceVector otherPv = new PresenceVector(kmerLength, residues.length);
		otherPv.setKmer(3); otherPv.setKmer(2);
		
		PresenceVector intersectPv = otherPv.union(pv);
		assertTrue(intersectPv.containsKmer(1) && intersectPv.containsKmer(2) &&
				intersectPv.containsKmer(3) && !intersectPv.containsKmer(4));
	}
}
