package cbcb.kmulus.allpairs.comparison;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Preconditions;

/**
 * A simple abstract {@link Mapper} which takes an input value, parses its id as defined by
 * {@link #parseId(Object)}, and maps it to one or more groups, as defined by
 * {@link #getGroups(long)}.
 * 
 * @author CH Albach
 *
 * @param <K1> the input key, which is unused by this {@link Mapper}
 * @param <V> the input and output value for this {@link Mapper}, the type of the item to compare
 */
public abstract class AllPairsMapper<K1, V> extends Mapper<K1, V, LongWritable, V> {
	
	/** Parses the 0 based internal identifier for this item. */
	protected abstract long parseId(K1 key, V value) throws IOException;
	
	/** Returns the {@link ExhaustiveUniqueGrouper} which will partition the keys. */
	protected abstract ExhaustiveUniqueGrouper getGrouper() throws IOException;		
	
	/** 
	 * Calculates and returns the group ids which the given item must be mapped to.
	 * 
	 * @param id the 0 based internal identifier for an item
	 * @return the 0 based identifiers for the groups to which the given item belongs
	 */
	protected long[] getGroups(long id) throws IOException {
		ExhaustiveUniqueGrouper grouper = Preconditions.checkNotNull(getGrouper());
		return grouper.getGroups(id);
	}
	
	@Override
	public void map(K1 key, V value, Context context) throws IOException, InterruptedException {
		long id = parseId(key, value);
		long[] groups = getGroups(id);
		
		for(int i = 0; i < groups.length; i++) {
			context.write(new LongWritable(groups[i]), value);
		}
	}
}