package cbcb.kmulus.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * A bit vector in which each index corresponds to the presence of a specific k-mer in a given 
 * sequence. Provides several basic utility functions for doing fast lookups, unions and 
 * intersections.
 * 
 * @author CH Albach
 */
public class PresenceVector implements Writable {
	
	/** A lookup for the bit counts of all possible bytes. */
	private static final byte[] bitCountLookup;
	static {
		bitCountLookup = new byte[1 << Byte.SIZE];
		for (int i = 0; i < bitCountLookup.length; i++) {
			byte count = 0;
			for (int j = i; j != 0; j >>>= 1) {
				count += j & 1;
			}
			bitCountLookup[i] = count;
		}
	}
	
	private int[] bits;
	private long id;
	
	/** Constructor for de-serialization purposes. */
	public PresenceVector() {
		bits = new int[0];
		id = -1;
	}
	
	/**
	 * Creates a general {@link PresenceVector} for a protein with an alphabet of the amino acids.
	 * 
	 * @param kmerLength length of the kmers indexed by this vector
	 */
	public PresenceVector(int kmerLength) {
		this(kmerLength, Biology.AMINO_ACIDS.length);
	}
	
	/**
	 * Creates a {@link PresenceVector} for k-mers of the given alphabet size.
	 * 
	 * @param kmerLength length of the kmers indexed by this vector
	 * @param alphabetSize size of the alphabet used for the kmers
	 */
	public PresenceVector(int kmerLength, int alphabetSize) {
		this(kmerLength, alphabetSize, -1);
	}	
	
	/**
	 * Creates a {@link PresenceVector} for k-mers of the given alphabet size.
	 * 
	 * @param kmerLength length of the k-mers indexed by this vector
	 * @param alphabetSize size of the alphabet used for the k-mers
	 * @param id the id for the sequence represented by this vector
	 */
	public PresenceVector(int kmerLength, int alphabetSize, long id) {
		int vectorLength = (int) Math.ceil(Math.pow(alphabetSize, kmerLength) / Integer.SIZE);
		this.bits = new int[vectorLength];
		this.id = id;
	}

	/**
	 * Copy constructor.
	 * 
	 * @param other vector to be copied
	 */
	public PresenceVector(PresenceVector other) {
		this.bits = new int[other.bits.length];
		for (int i = 0; i < bits.length; i++) {
			this.bits[i] = other.bits[i];
		}
	}
	
	/**
	 * Returns a set of the hashes of all k-mers present in the vector. This set will not reflect
	 * any changes made to the vector after the call to this function.
	 * 
	 * @return a set of all present k-mer hashes
	 */
	public Set<Integer> getAllPresentHashes() {
		Set<Integer> hashes = new HashSet<Integer>();
		for (int i = 0; i < bits.length * Integer.SIZE; i++) {
			if (containsKmer(i)) {
				hashes.add(i);
			}
		}
		return hashes;
	}
	
	/**
	 * Sets the given kmerIndex to present. Equivalent to calling {@link #setKmer(int, boolean)}
	 * with present set to 'true'.
	 * 
	 * @param kmerIndex the binary index of the kmer to be set
	 */
	public void setKmer(int kmerIndex) {
		setKmer(kmerIndex, true);
	}
	
	/**
	 * Sets the given kmerIndex to present or not present.
	 * 
	 * @param kmerIndex the binary index of the kmer to be set
	 * @param present the value which the index should be set to
	 */
	public void setKmer(int kmerIndex, boolean present) {
		int bit = 1 << (kmerIndex % Integer.SIZE);
		if (present) {
			bits[kmerIndex / Integer.SIZE] |= bit;
		
		} else {
			bits[kmerIndex / Integer.SIZE] &= ~bit;
		}
	}
	
	/**
	 * Checks if the given kmer is present in the vector.
	 * 
	 * @param kmerIndex the binary index of the kmer to be checked, dependent on the alphabet
	 * @return true if the kmer is present, false otherwise
	 */
	public boolean containsKmer(int kmerIndex) {
		int chunk = bits[kmerIndex / Integer.SIZE];
		int mask = 1 << (kmerIndex % Integer.SIZE);
		return (chunk & mask) > 0;
	}
	
	/** Sets the (optional) id for the sequence associated with this {@link PresenceVector}. */
	public void setId(long id) {
		this.id = id;
	}
	
	/** Returns the (optional) id for the sequences associated with this {@link PresenceVector}. */
	public long getId() {
		return id;
	}
	
	/**
	 * Checks if this intersects with the given {@link PresenceVector}.
	 * 
	 * @param other the vector to check against
	 * @return true if the vectors intersect, false otherwise
	 */
	public boolean intersects(PresenceVector other) {
		Preconditions.checkNotNull(other);
		Preconditions.checkState(hasSameParameters(other));
		
		for (int i = 0; i < bits.length; i++) {
			if ((bits[i] & other.bits[i]) > 0) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Intersects the bits of itself with the given {@link PresenceVector}.  This vector is
	 * unchanged as a result of this operation.  Equivalent to '&'.
	 * 
	 * @param other the vector which is intersected
	 * @return a new {@link PresenceVector} which is the result of this operation
	 */
	public PresenceVector intersect(PresenceVector other) {
		Preconditions.checkNotNull(other);
		Preconditions.checkState(hasSameParameters(other));
		
		PresenceVector result = new PresenceVector(this);
		return result.intersectEquals(other);
	}
	
	/**
	 * Intersects the bits of itself with the given {@link PresenceVector}.  Assigns the resulting
	 * bits to this vector.  Equivalent to '&='.
	 * 
	 * @param other the vector which is intersected
	 * @return this vector
	 */
	public PresenceVector intersectEquals(PresenceVector other) {
		Preconditions.checkNotNull(other);
		Preconditions.checkState(hasSameParameters(other));
		
		for (int i = 0; i < bits.length; i++) {
			bits[i] &= other.bits[i];
		}
		return this;
	}
	
	/**
	 * Counts the number of intersecting k-mers between this and the given {@link PresenceVector}.
	 * 
	 * @param other the vector to compare to
	 * @return the number of overlapping k-mers between the two vectors
	 */
	public int intersectionCount(PresenceVector other) {
		Preconditions.checkNotNull(other);
		
		int count = 0;
		for (int i = 0; i < bits.length; i++) {
			count += countBits(bits[i] & other.bits[i]);
		}
		return count;
	}
	
	/**
	 * Computes the Hamming distance, the number of bitwise differences between the two vectors.
	 * 
	 * @param vector the vector to be compared against
	 * @return the Hamming distance
	 */
	public int getHammingDistance(PresenceVector other) {
		Preconditions.checkNotNull(other);
		
		int count = 0;
		for (int i = 0; i < bits.length; i++) {
			count += countBits(bits[i] ^ other.bits[i]);
		}
		return count;
	}
	
	/**
	 * Counts the bits in the given int.
	 * 
	 * @param value integer to be counted
	 * @return the number of bits in the given int
	 */
	int countBits(int value) {
		int count = 0;
		for (int i = 0; i < Integer.SIZE; i += Byte.SIZE) {
			int lastByte = (value >>> i) & 0xFF;
			count += bitCountLookup[lastByte];
		}
		return count;
	}
	
	/**
	 * Takes the union of this with the given {@link PresenceVector} and generates a new vector
	 * with the results.  This vector is unchanged as a result of this operation.  Equivalent to
	 * '|'.
	 * 
	 * @param other the vector which is unioned
	 * @return a new {@link PresenceVector} which is the result of this operation
	 */
	public PresenceVector union(PresenceVector other) {
		Preconditions.checkNotNull(other);
		Preconditions.checkState(hasSameParameters(other));
		
		PresenceVector result = new PresenceVector(this);
		return result.unionEquals(other);
	}
	
	/**
	 * Takes the union of this with the given {@link PresenceVector} and stores the resulting bits
	 * in this vector.  Equivalent to '|='.
	 * 
	 * @param other the vector which is unioned
	 * @return this vector
	 */
	public PresenceVector unionEquals(PresenceVector other) {
		Preconditions.checkNotNull(other);
		Preconditions.checkState(hasSameParameters(other));
		
		for (int i = 0; i < bits.length; i++) {
			bits[i] |= other.bits[i];
		}
		return this;
	}
	
	boolean hasSameParameters(PresenceVector o) {
		return o != null && bits.length == o.bits.length;
	}
	
	@Override
	public int hashCode() {
		int hash = 0;
		for (int i = 0; i < bits.length; i++) {
			hash ^= bits[i];
		}
		return hash;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		
		} else if (!(o instanceof PresenceVector)) {
			return false;
		
		} else {
			PresenceVector pv = (PresenceVector) o;
			if (id != pv.id || bits.length != pv.bits.length) {
				return false;
			}
			
			for (int i = 0; i < bits.length; i++) {
				if (bits[i] != pv.bits[i]) {
					return false;
				}
			}
			return true;
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readLong();

		int length = in.readInt();
		if (bits.length != length) {
			bits = new int[length];
		}
		
		for (int i = 0; i < length; i++) {
			bits[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeInt(bits.length);
		for (int i = 0; i < bits.length; i++) {
			out.writeInt(bits[i]);
		}
	}
}
