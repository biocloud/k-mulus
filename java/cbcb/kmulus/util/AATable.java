package cbcb.kmulus.util;

import java.util.Map;

import com.google.common.base.Preconditions;

/** 
 * A table for converting DNA triplets to their amino acid translation equivalents. The default
 * implementation of this table may be found at {@link Biology#defaultTable}.
 */
public class AATable {

	private final Map<String, Character> aaMap;

	public AATable(Map<String, Character> aaMap) {
		this.aaMap = Preconditions.checkNotNull(aaMap);

		for (String triplet : Biology.TRIPLETS) {
			Preconditions.checkNotNull(aaMap.get(triplet));
		}
	}

	public char get(String dnaTriplet) {
		return aaMap.get(dnaTriplet);
	}
}
