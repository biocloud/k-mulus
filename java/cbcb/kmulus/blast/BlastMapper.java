package cbcb.kmulus.blast;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import protein.util.Biology;

/**
 * Mapper recieves a chunk of input simple fasta sequences. For each k-mer seen in
 * the query sequence, it is checked against each clusters k-mers vector.  If
 * the query shares a k-mer in common with a cluster, the (cluster_id, >query_seq)
 * is emitted.
 */
public class BlastMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private static final Logger LOG = Logger.getLogger(BlastMapper.class);

	private int alphabetSize;
	private int kmerLength;
	private int numberOfBytes;
	private int numCenters;
	
	// K-mer presence vector.
	public static byte[] clusterKmerVectors;
	
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		kmerLength = conf.getInt(Blast.KMER_LENGTH, 3);
		alphabetSize = conf.getInt(Blast.ALPHABET_SIZE, Biology.AMINO_ACIDS.length);
		numberOfBytes = (int) Math.ceil(Math.pow(alphabetSize, kmerLength) / 8);
		
		// Load each cluster k-mer bit vectors into memory.
		String clusterDir = conf.get(Blast.CLUSTER_DIR);
		if (!loadClusters(clusterDir))
			LOG.info("Loading clusters from " + clusterDir + " has failed.");
	}
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		// Toy example, just send the sequence to the clusterID.
		String[] clusterAndSequence = value.toString().trim().split("\t");
		
		context.write(new LongWritable(new Long(clusterAndSequence[0])),
				new Text(clusterAndSequence[1]));
	}
}
