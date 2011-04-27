package cbcb.kmulus.allpairs.comparison;

/** Represents a projective plane with prime order. */
public class ClassicalPlaneRot extends PrimeRot {

	public ClassicalPlaneRot(long numItems, int p) {

		/*In a classical plane, the order is prime.*/
		super(numItems, p, 1, p);
	}

	protected void initializeIids() {
		//do nothing...
	}

	@Override
	public long[] getGroups(long id) {
		long[] groups = new long[p+1];

		/*If it is the last item in the set, it belongs to all indicator groups.*/
		if(id == n*n + n) { 
			for(int i = 0; i < groups.length; i++) {
				groups[i] = i;
			}
			
			return groups;
		}

		/*Determine the indicator group this id belongs to.*/
		long indicatorGroup = id / p;
		groups[0] = indicatorGroup;

		/*Determine the row groups.*/
		if(indicatorGroup == 0) {

			long offset = id * p + (p + 1);

			for(int i = 0; i < p; i++) {
				groups[i+1] = offset + i;
			}

		} else {

			int offset = p + 1;

			/*Track the un-modulated index of the item within its indicator group*/
			long index = id; 

			for(int i = 0; i < p; i++) {

				groups[i+1] = offset + ( index % p );

				/*Rotate based on the indicator group number.*/
				index += indicatorGroup;

				/*Head down to the next set of row groups.*/
				offset += p;
			}
		}

		return groups;
	}
}
