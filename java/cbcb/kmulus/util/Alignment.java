package cbcb.kmulus.util;

/** Represents an alignment between two amino acid or DNA sequences. */
public interface Alignment {

	/** Returns the distance between the two sequences. */
	public long getDistance();
}
