package cbcb.kmulus.allpairs.comparison;

import java.io.IOException;

/**
 * In this algorithm, items are simply grouped into pairs of two.
 * Given n items, this will generate n(n - 1)/2 groups.
 * 
 * @author CH Albach
 */
public class PairSplit implements ExhaustiveUniqueGrouper {

	private int numItems;
	
	public PairSplit(long numItems) throws IOException {
		if(numItems > Integer.MAX_VALUE)
			throw new IOException("Number of items exceeded the capacity for a PairSplit.");
		
		this.numItems = (int) numItems;
	}
	
	public long getNumItems() {
		return numItems;
	}
	
	/**
	 * Returns group ids for id paired with every other item.
	 */
	public long[] getGroups(long id) {
		long[] groups = new long[numItems-1];
		
		for(int i = 0; i < id; i++) {
			groups[i] = i * numItems + id;
		}
		
		for(int i = (int)id + 1; i < numItems; i++) {
			groups[i-1] = id * numItems + i;
		}
	
		return groups;
	}
	
	public long getNumGroups() {
		return (numItems * (numItems + 1)) / 2;
	}
	
}
