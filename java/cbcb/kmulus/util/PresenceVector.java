package cbcb.kmulus.util;

import com.google.common.base.Preconditions;

import cbcb.kmulus.util.Biology;

/**
 * A bit vector in which each index corresponds to the presence of a specific k-mer in a given 
 * sequence. Provides several basic utility functions for doing fast lookups, unions and 
 * intersections.
 * 
 * @author CH Albach
 */
public class PresenceVector {
	
	private static final int NO_RESIDUE = -1;
	
	private final int kmerLength;
	private final int[] bits;
	
	/** A case insensitive mapping of residue characters to lookup indices. */
	private final int[] residueLookup;
	
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
		this.kmerLength = kmerLength;
		this.bits = new int[vectorLength];
		
		this.residueLookup = new int[Character.MAX_VALUE + 1];
		for (int i = 0; i < residueLookup.length; i++) {
			residueLookup[i] = NO_RESIDUE;
		}
		for (int i = 0; i < residues.length; i++) {
			residueLookup[Character.toLowerCase(residues[i])] = i;
			residueLookup[Character.toUpperCase(residues[i])] = i;
		}
	}	

	/**
	 * Copy constructor.
	 * 
	 * @param other vector to be copied
	 */
	public PresenceVector(PresenceVector other) {
		this.kmerLength = other.kmerLength;
		this.bits = new int[other.bits.length];
		for (int i = 0; i < bits.length; i++) {
			this.bits[i] = other.bits[i];
		}
	
		this.residueLookup = new int[other.residueLookup.length];
		for (int i = 0; i < residueLookup.length; i++) {
			this.residueLookup[i] = other.residueLookup[i];
		}
	}
	
	/**
	 * Checks if the given kmer is present in the vector.
	 * 
	 * @param kmer the kmer to be checked
	 * @return true if the kmer is present, false otherwise
	 */
	public boolean containsKmer(String kmer) {
		Preconditions.checkNotNull(kmer);
		Preconditions.checkState(kmer.length() == kmerLength);
		
		int index = 0;
		int positionValue = 1;
		for (int i = 0; i < kmerLength; i++) {
			int code = residueLookup[kmer.charAt(i)];
			index += code * positionValue;
			positionValue *= kmerLength;
		}
		return indexPresent(index);
	}
	
	/**
	 * Determines if the given index is a 1 or a 0.
	 * 
	 * @param index the index to be checked
	 * @return true if the index is 1, false if 0
	 */
	private boolean indexPresent(int index) {
		int chunk = bits[index / Integer.MAX_VALUE];
		int mask = 1 << (index % Integer.MAX_VALUE);
		return (chunk & mask) > 0;
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
		return o != null && kmerLength == o.kmerLength && bits.length == o.bits.length;
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
}
