package cbcb.kmulus.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Set;

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

	/** Test for {@link PresenceVector#getAllPresentHashes()} */ 
	public void testGetAllPresentHashes() {
		PresenceVector pv = new PresenceVector(2);
		
		pv.setKmer(Biology.getAAKmerHash("AA"));
		pv.setKmer(Biology.getAAKmerHash("U*"));
		pv.setKmer(Biology.getAAKmerHash("VK"));
		
		Set<Integer> presentHashes = pv.getAllPresentHashes();
		assertEquals(3, presentHashes.size());
		assertTrue(presentHashes.contains(Biology.getAAKmerHash("AA")));
		assertTrue(presentHashes.contains(Biology.getAAKmerHash("U*")));
		assertTrue(presentHashes.contains(Biology.getAAKmerHash("VK")));
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
	
	/** 
	 * Test for {@link PresenceVector#readFields(java.io.DataInput)} and 
	 *  {@link PresenceVector#write(java.io.DataOutput)}. 
	 */
	public void testSerialization() throws Exception {
		PipedInputStream pipedIn = new PipedInputStream();
		PipedOutputStream pipedOut = new PipedOutputStream();
		pipedIn.connect(pipedOut);
		
		DataInput in = new DataInputStream(pipedIn);
		DataOutput out = new DataOutputStream(pipedOut);
		
		PresenceVector pv = new PresenceVector(2);
		pv.setKmer(Biology.getAAKmerHash("AA"));
		pv.setKmer(Biology.getAAKmerHash("U*"));
		pv.setKmer(Biology.getAAKmerHash("VK"));
		pv.write(out);
		
		PresenceVector pv2 = new PresenceVector();
		pv2.readFields(in);
		
		assertEquals(pv, pv2);
	}
}
