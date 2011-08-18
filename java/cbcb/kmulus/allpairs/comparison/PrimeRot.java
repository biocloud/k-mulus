package cbcb.kmulus.allpairs.comparison;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the prime-rot algorithm which produces unique groupings within a set. These
 * groupings are unique in that over all groups, each pair of items are found in the same group
 * exactly once. 
 * 
 * This algorithm works optimally on sets of size p^2 + p + 1, where p is a prime number.
 * As p^2 + p + 1 is the exact capacity for this algorithm, in sub-optimal cases p will be chosen as
 * the smallest possible prime number such that p^2 + p + 1 >= set size.
 * 
 * @author CH Albach
 */
public abstract class PrimeRot implements ExhaustiveUniqueGrouper {

	/** The size of the input set of items. */
	private long numItems;
	
	/** The prime number associated with the algorithm. */
	protected int p;
	
	/** The exponent of the prime number. */
	protected int k;
	
	/** The order of the projective plane (p^power). */
	protected int n;
		
	public PrimeRot(long numItems, int p, int k, int n) {
		this.numItems = numItems;
		this.p = p;
		this.k = k;
		this.n = n;
	}
	
	public long getNumItems() {
		return numItems;
	}
	
	public long getNumGroups() {
		return n + 1;
	}
	
	public int getP() {
		return p;
	}
	
	public int getK() {
		return k;
	}
	
	public int getN() {
		return n;
	}
	
	public static PrimeRot generatePrimeRot(long numItems, int p) {
		return new ClassicalPlaneRot(numItems, p);
	}
	
	/**
	 * Determine the smallest order projective plane which can fit the desired input set size. The
	 * order of this plane may be p^k where p is prime and k is an integer.  Planes where k=1 are
	 * called Classical projective planes or Desarguesian^2. If such a plane is optimal, this method
	 * returns a {@link ClassicalPlaneRot} implementation of {@link PrimeRot}.  
	 * 
	 * TODO(CH): Planes of other powers should create a {@link NonclassicalPlaneRot} implementation.
	 */
	public static PrimeRot generatePrimeRot(long numItems) {
		
		List<Integer> primes = new ArrayList<Integer>();
		int prime = 1;
		int p, k, n;
		
		/*Compute the smallest possible classical projective plane.*/
		while(prime * prime + prime + 1 < numItems) {
			
			do {
				prime++;
			} while(!isPrime(prime, primes));
			
			primes.add(prime);
		}
		
		/*Store the classical plane data.*/
		p = prime;
		k = 1;
		n = prime;
		
		return new ClassicalPlaneRot(numItems, p);
		
		//System.out.println("p: " + p + ", k: " + k + ", n: " + n);
		//System.out.println("classical capacity: " + (n*n + n + 1));
		/*TODO put in a terminating condition for orders which have a capacity
		 * within a certain distance of numItems
		 */
		
		/* See if there are any 'tighter fitting' non-classical planes.
		 * 
		 * If p^2k + p^k + 1 >= numItems, where p is a prime and k is an integer;
		 * for each prime, the lowest value of k is FLOOR[ logp(numItems-1)/2 ].  
		 * (This can be derived from p^2k + p^k + 1 <= numItems) */
		
		/*Compute the reusable numerator of the exponent.*/
		/*double expNum = Math.log(numItems-1)/2;
		
		for(Integer primeFactor : primes) {
			
			/*Compute the lowest possible order allowed by numItems.*/
			/*int exp = (int)( expNum / Math.log(primeFactor) );
			int order = (int)Math.pow(primeFactor, exp);
			
			/*Increment the exponent until the plane can fit numItems.*/
			/*while(order * order + order + 1 < numItems) {
				/*Determine subsequent powers.*/
				/*order *= primeFactor;
				exp++;
			}
			
			/*If this order beats out the old one, use it.  Careful of overflow.*/
			/*if(order > 0 && order < n) {
				p = primeFactor;
				k = exp;
				n = order;
			}
		}
		
		if(k == 1) {
			return new ClassicalPlaneRot(numItems, p);
		} else {
			return new NonclassicalPlaneRot(numItems, p, k, n);
		}*/
	}
	
	/**
	 * Map out internal ids such that placement for the algorithm is optimal.
	 * Assuming that the input is zero based and is complete from 0 - numItems
	 * 
	 * Eventually this could take into account the size of each item.
	 */
	protected abstract void initializeIids();
	
	/**
	 * Uses the prime-rot algorithm to calculate the groups to which the ID 
	 * belongs.
	 */
	public abstract long[] getGroups(long id);
		
	
	/**
	 * Given a list of preceding prime numbers, determines if p is prime.
	 */
	private static boolean isPrime(int p, List<Integer> primeList) {
		// TODO: Use a better method of primality testing.
		int lim = (int)Math.sqrt(p);
		
		for(int prime : primeList) {
			if(prime > lim)
				break;
			
			if(p % prime == 0) {
				return false;
			}
		}
		
		return true;
	}
	
}
