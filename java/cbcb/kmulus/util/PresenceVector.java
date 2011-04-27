package cbcb.kmulus.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
	
	private int[] bits;
	
	/** Constructor for deserialization purposes. */
	public PresenceVector() {
		bits = new int[0];
	}
	
	/**
	 * Creates a general {@link PresenceVector} for a protein.  Uses the standard alphabet for amino
	 * acids.
	 * 
	 * @param kmerLength length of the kmers indexed by this vector
	 */
	public PresenceVector(int kmerLength) {
		this(kmerLength, Biology.AMINO_ACIDS);
	}
	
	/**
	 * Creates a {@link PresenceVector} for the given array of residues or alphabet.
	 * 
	 * @param kmerLength length of the kmers indexed by this vector
	 * @param residues all possible residues for a given position, or a compressed alphabet
	 */
	public PresenceVector(int kmerLength, char[] residues) {
		int vectorLength = (int) Math.ceil(Math.pow(residues.length, kmerLength) / Integer.SIZE);
		this.bits = new int[vectorLength];
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
		int bit = 1 << (kmerIndex % Integer.MAX_VALUE);
		if (present) {
			bits[kmerIndex / Integer.MAX_VALUE] |= bit;
		
		} else {
			bits[kmerIndex / Integer.MAX_VALUE] &= ~bit;
		}
	}
	
	/**
	 * Checks if the given kmer is present in the vector.
	 * 
	 * @param kmerIndex the binary index of the kmer to be checked, dependent on the alphabet
	 * @return true if the kmer is present, false otherwise
	 */
	public boolean containsKmer(int kmerIndex) {
		int chunk = bits[kmerIndex / Integer.MAX_VALUE];
		int mask = 1 << (kmerIndex % Integer.MAX_VALUE);
		return (chunk & mask) > 0;
	}
	
	/**
	 * Checks if this intersects with the given {@link PresenceVector}.
	 * 
	 * @param other the vector to check against
	 * @return true if the vectors intersect, false otherwise
	 */
	public boolean intersects(PresenceVector other) {
		Preconditions.checkNotNull(other);
		Preconditions.checkState(sameParameters(other));
		
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
		Preconditions.checkState(sameParameters(other));
		
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
		Preconditions.checkState(sameParameters(other));
		
		for (int i = 0; i < bits.length; i++) {
			bits[i] &= other.bits[i];
		}
		return this;
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
		Preconditions.checkState(sameParameters(other));
		
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
		Preconditions.checkState(sameParameters(other));
		
		for (int i = 0; i < bits.length; i++) {
			bits[i] |= other.bits[i];
		}
		return this;
	}
	
	public boolean sameParameters(PresenceVector o) {
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
			if (bits.length != pv.bits.length) {
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
		out.writeInt(bits.length);
		for (int i = 0; i < bits.length; i++) {
			out.writeInt(i);
		}
	}
}
