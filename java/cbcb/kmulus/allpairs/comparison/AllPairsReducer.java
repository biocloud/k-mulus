package cbcb.kmulus.allpairs.comparison;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A computationally intensive {@link Reducer} which computes all pairs comparisons between its
 * set of input values.
 * 
 * @author CH Albach
 *
 * @param <V2> the input type of item to be compared
 * @param <V3> an emitted value representing the comparison between two items
 */
public abstract class AllPairsReducer<V2, V3> extends Reducer<LongWritable, V2, Text, V3> {

	public static final char SEQ_ID_DELIM = ',';
	
	/** Parses the 0 based internal identifier for this item. */
	protected abstract long parseId(V2 value) throws IOException;
	
	/** Compares the given items. */
	protected abstract V3 compareItems(V2 a, V2 b);
	
	/** Creates a copy of the given item by value. */
	protected abstract V2 copyValue(V2 original);
	
	@Override
	public void reduce(LongWritable key, Iterable<V2> values, Context context)
			throws IOException, InterruptedException {
		List<V2> list = new ArrayList<V2>();
		for (V2 v : values) {
			list.add(copyValue(v));
		}

		for(int i = 0; i < list.size(); i++) {
			long idA = parseId(list.get(i));
			
			for(int j = i + 1; j < list.size(); j++) {
				long idB = parseId(list.get(j));
				Text idConcat = new Text();
				
				if(idA < idB) {
					idConcat.set("" + idA + SEQ_ID_DELIM + idB);
				} else {
					idConcat.set("" + idB + SEQ_ID_DELIM + idA);
				}
				
				V3 score = compareItems(list.get(i), list.get(j));
				context.write(idConcat, score);
				context.progress();
			}
		}		
	}
}
