package cbcb.kmulus.allpairs.protein;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** TODO(calbach): Justify this class's existence. */
public class KmerAligner extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) {
			
		}
	}
	
	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO(calbach): Hadoop setup.
		return 0;
	}

	public static void main(String[] args) {
		int result = 1;

		try {
			result = ToolRunner.run(new KmerAligner(), args);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Job failed.");
		}
		System.exit(result);
	}
}
