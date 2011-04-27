package cbcb.kmulus.allpairs.comparison;

/**
 * Represents a projective plane of the order of any prime power. 
 * 
 * TODO(calbach): Find out how to construct planes of this power.
 * 
 * @author CH Albach
 */
public class NonclassicalPlaneRot extends PrimeRot {

	public NonclassicalPlaneRot(long numItems, int p, int k, int n) {
		super(numItems, p, k, n);
	}

	protected void initializeIids() {
		// do nothing
	}

	/**
	 * Uses the prime-rot algorithm to calculate the groups to which the ID 
	 * belongs.
	 */
	public long[] getGroups(long id) {
		long[] groups = new long[n+1];

		/*If it is the last item in the set, it belongs to all indicator groups.*/
		if(id == n*n + n) { 
			for(int i = 0; i < groups.length; i++) {
				groups[i] = i;
			}

			return groups;
		}

		/*Determine the indicator group this id belongs to.*/
		int indicatorGroup = (int)(id / n);
		groups[0] = indicatorGroup;

		/*The 0th indicator group is distributed in a unique manner.*/
		if(indicatorGroup == 0) {

			long offset = id * n + (n + 1);

			for(int i = 0; i < n; i++) {
				groups[i+1] = offset + i;
			}

			/*The first indicator group will be distributed without rotation.*/
		} else if(indicatorGroup == 1) {

			long offset = id % n + (n + 1);

			for(int i = 0; i < n; i++) {
				groups[i+1] = offset + i*n;
			}



		} else {

			/*Keep the standard implementation for the first set of row groups.*/
			groups[1] = n + 1 + (id % n);


			/* ************************************************************ *
			 * This is where it gets non-classical. It involves a 'recursive' 
			 * application of the PrimeRot algorithm.  
			 * 
			 * View each indicator group as nested sets of p groups (where 
			 * there are k levels to the nesting).  A rotation factor will be
			 * composed of k parts.  Each part rotates a corresponding kth level
			 * of nested groups.  
			 * ************************************************************ */

			/*Skip the indicator groups and the first set of row groups.*/
			int offset = 2*n + 1;

			/*Original position of id within its group.*/
			long modId = id;
			int[] pos = new int[k];

			/*Store in 'fixed' base p notation, for faster processing.*/
			for(int i = 0; i < k; i++) {
				pos[i] = (int) (modId % p);
				modId /= p;
			}

			for(int i = 0; i < n-1; i++) {
				int rotFactor = indicatorGroup + i - 1;
				
				/*Excludes 0 from rotFactors.*/
				if (rotFactor >= n)
					rotFactor++;

				/*Offset from the start of the set of groups.*/
				int inset = 0;
				
				/*The value of the current position in base p.*/
				int expP = 1;
				
				/*Add each rot factor to the position in base p notation.*/
				for(int j = 0; j < k; j++) {
					inset += ((pos[j] + rotFactor) % p) * expP;
					rotFactor /= p;
					
					expP *= p;
				}
				
				/*Group id based on offset from group 0 and offset within set.*/
				groups[i+2] = offset + inset;
				
				/*Advance to the next set of row groups.*/
				offset += n;
			}
		}

		return groups;
	}


}
