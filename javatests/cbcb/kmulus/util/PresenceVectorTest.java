package cbcb.kmulus.util;

import cbcb.kmulus.util.PresenceVector;
import cbcb.kmulus.util.Biology;

import junit.framework.TestCase;

/** Tests for {@link PresenceVector}. */
public class PresenceVectorTest extends TestCase {
	protected int kmerLength = 2;
	protected int alphabetSize = 3;
	
	/** Test for {@link PresenceVector#PresenceVector(int, char[])} */
	public void testPresenceVector() {
		PresenceVector pv = new PresenceVector(kmerLength, alphabetSize);
		assertTrue(pv != null);
	}
	
	
	/** Test for {@link PresenceVector#setKmer(int)} */
	public void testSetKmer() {
		PresenceVector pv = new PresenceVector(kmerLength, alphabetSize);
	
		pv.setKmer(0);
		assertTrue(pv.containsKmer(0));
		assertFalse(pv.containsKmer(1));
		
		pv.setKmer(1);
		assertTrue(pv.containsKmer(1));
	}
	
	/** Test for {@link PresenceVector#setKmer(int)} */
	public void testSetKmerBiology() {
		PresenceVector pv = new PresenceVector(kmerLength, Biology.AMINO_ACIDS.length);
	
		pv.setKmer(Biology.getCompressedHash("AA"));
		assertTrue(pv.containsKmer(Biology.getCompressedHash("AA")));
		assertFalse(pv.containsKmer(Biology.getCompressedHash("AV")));
		
		pv.setKmer(Biology.getCompressedHash("VV"));
		pv.setKmer(Biology.getCompressedHash("AA"), false);

		assertTrue(pv.containsKmer(Biology.getCompressedHash("VV")));
		assertFalse(pv.containsKmer(Biology.getCompressedHash("AA")));
	}
	
	/** Test for {@link PresenceVector#PresenceVector(PresenceVector)} */
	public void testCopyConstructor() {
		PresenceVector pv = new PresenceVector(kmerLength, alphabetSize);
		pv.setKmer(0); pv.setKmer(1); pv.setKmer(4);
		
		PresenceVector otherPv = new PresenceVector(pv);
		pv.setKmer(0, false);
		
		assertTrue(otherPv.containsKmer(0) && otherPv.containsKmer(1) &&
				otherPv.containsKmer(4));
	}
	
	/** Test for {@link PresenceVector#intersect(PresenceVector)} */
	public void testIntersect() {
		PresenceVector pv = new PresenceVector(kmerLength, alphabetSize);
		pv.setKmer(2); pv.setKmer(1); pv.setKmer(4);
		
		PresenceVector otherPv = new PresenceVector(kmerLength, alphabetSize);
		otherPv.setKmer(3); otherPv.setKmer(5); otherPv.setKmer(4);
		
		PresenceVector intersectPv = otherPv.intersect(pv);
		assertTrue(intersectPv.containsKmer(4) && !intersectPv.containsKmer(2) &&
				!intersectPv.containsKmer(5));
	}
	
	/** Test for {@link PresenceVector#intersectEquals(PresenceVector)} */
	public void testIntersectEquals() {
		PresenceVector pv = new PresenceVector(kmerLength, alphabetSize);
		pv.setKmer(2); pv.setKmer(1); pv.setKmer(4);
		
		PresenceVector otherPv = new PresenceVector(kmerLength, alphabetSize);
		otherPv.setKmer(3); otherPv.setKmer(5); otherPv.setKmer(4);
		
		PresenceVector intersectPv = otherPv.intersectEquals(pv);
		assertTrue(intersectPv.containsKmer(4) && !intersectPv.containsKmer(3));
	}
	
	/** Test for {@link PresenceVector#intersectionCount(PresenceVector)} */
	public void testIntersectCount() {
		PresenceVector pv = new PresenceVector(kmerLength, 20);
		pv.setKmer(10); pv.setKmer(69); pv.setKmer(100);
		
		PresenceVector otherPv = new PresenceVector(kmerLength, 20);
		otherPv.setKmer(10); otherPv.setKmer(100); otherPv.setKmer(101);
		assertEquals(2, pv.intersectionCount(otherPv));
	}
	
	/** Test for {@link PresenceVector#countBits(int)}. */
	public void testCountBits() {
		PresenceVector vector = new PresenceVector();
		assertEquals(0, vector.countBits(0x00));
		assertEquals(1, vector.countBits(0x01));
		assertEquals(2, vector.countBits(0x05));
		assertEquals(4, vector.countBits(0x0F));
		assertEquals(4, vector.countBits(0xF0));
		assertEquals(1, vector.countBits(0x80));
		assertEquals(8, vector.countBits(0xFF));
	}
	
	/** Test for {@link PresenceVector#union(PresenceVector)} */
	public void testUnion() {
		PresenceVector pv = new PresenceVector(kmerLength, alphabetSize);
		pv.setKmer(2); pv.setKmer(1);
		
		PresenceVector otherPv = new PresenceVector(kmerLength, alphabetSize);
		otherPv.setKmer(3); otherPv.setKmer(2);
		
		PresenceVector intersectPv = otherPv.union(pv);
		assertTrue(intersectPv.containsKmer(1) && intersectPv.containsKmer(2) &&
				intersectPv.containsKmer(3) && !intersectPv.containsKmer(4));
	}
}
