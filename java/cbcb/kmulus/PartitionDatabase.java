package cbcb.kmulus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import cbcb.kmulus.db.cluster.ClusterPresenceVectors;
import cbcb.kmulus.db.processing.GenerateSequencePresenceVectors;
import cbcb.kmulus.db.processing.PrepareClusteringOutput;
import cbcb.kmulus.db.processing.UnionClusterPresenceVectors;
import cbcb.kmulus.db.processing.WriteSequencesToCluster;

/**
 * The pipeline for generating clustered database partitions from a single database.
 * 
 * @author CH Albach
 */
public class PartitionDatabase {
	
	private static final String USAGE = "PartitionDatabase DATABASE_SEQS OUTPUT_DIR NUM_SEQ NUM_CLUSTERS [KMER_LEN]";
	
	/* Final output directories. */
	private static final String PARTITIONS_SUFFIX = "partitions";
	private static final String CENTERS_SUFFIX = "centers";
	
	/* Intermediate output directories. */
	private static final String TEMP_SUFFIX = "temp";
	private static final String GENERATE_SUFFIX = "gen";
	private static final String CLUSTER_SUFFIX = "cluster";
	private static final String PREP_SUFFIX = "prep";
	
	private static final String DEFAULT_KMER_LEN = "3";
	
	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println(USAGE);
			return;
		}
		
		String dbInput = args[0];
		String tempOut = args[1] + Path.SEPARATOR + TEMP_SUFFIX;
		String finalOut = args[1] + Path.SEPARATOR + PARTITIONS_SUFFIX;
		String numSeq = args[2];
		String numClusters = args[3];
		String kmerLen = args.length > 4 ? args[4] : DEFAULT_KMER_LEN;
		
		// Verify that the inputs are of the correct type.
		Integer.parseInt(numSeq);
		Integer.parseInt(numClusters);
		Integer.parseInt(kmerLen);
		
		try {
			// Delete the output directories.
			Configuration conf = new Configuration();
			FileSystem.get(conf).delete(new Path(tempOut), true);
            FileSystem.get(conf).delete(new Path(finalOut), true);
			
			// TODO(calbach): Repeat masking.

			// Transform the database sequences into PresenceVectors.
			String pvOut = tempOut + Path.SEPARATOR + GENERATE_SUFFIX;
			int result = ToolRunner.run(
					new GenerateSequencePresenceVectors(),
					new String[]{dbInput, pvOut, kmerLen});
			
			if (result < 0) {
				System.err.println(GenerateSequencePresenceVectors.class.getName() + " failed.");
				System.exit(result);
			}

			// Cluster the PresenceVectors.
			String clusterOut = tempOut + Path.SEPARATOR + CLUSTER_SUFFIX;
			int runIter = 0;
			do {
				result = ToolRunner.run(new Configuration(),
						new ClusterPresenceVectors(runIter),
						new String[]{pvOut, clusterOut, numSeq, numClusters});
				
				runIter++;
			} while (result == ClusterPresenceVectors.CODE_LOOP);
			
			if (result == ClusterPresenceVectors.CODE_CONVERGED) {
				result = ToolRunner.run(new Configuration(), 
						new ClusterPresenceVectors(),
						new String[]{pvOut, clusterOut, numSeq, numClusters});
			}
			
			if (result < 0) {
				System.err.println(ClusterPresenceVectors.class.getName() + " failed.");
				System.exit(result);
			}
			clusterOut += Path.SEPARATOR + ClusterPresenceVectors.FINAL_DIR;
			
			// Reformat the clustering output for partitioning.
			String prepClusterOut = tempOut + Path.SEPARATOR + PREP_SUFFIX;
			result = ToolRunner.run(
					new PrepareClusteringOutput(),
					new String[]{clusterOut, prepClusterOut});
			
			if (result < 0) {
				System.err.println(PrepareClusteringOutput.class.getName() + " failed.");
				System.exit(result);
			}
			
			// Generate the database partitions.
			String partitionsOut = finalOut + Path.SEPARATOR + PARTITIONS_SUFFIX;
			result = ToolRunner.run(
					new WriteSequencesToCluster(),
					new String[]{prepClusterOut, dbInput, partitionsOut, tempOut + Path.SEPARATOR + "null"});
			
			if (result < 0) {
				System.err.println(WriteSequencesToCluster.class.getName() + " failed.");
				System.exit(result);
			}

			// Generate a union vector as the center for each cluster.
			String centersOut = finalOut + Path.SEPARATOR + CENTERS_SUFFIX;
			result = ToolRunner.run(
					new UnionClusterPresenceVectors(),
					new String[]{clusterOut, centersOut});
			
			if (result < 0) {
				System.err.println(UnionClusterPresenceVectors.class.getName() + " failed.");
				System.exit(result);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Pipeline failed.");
		}
	}
}
