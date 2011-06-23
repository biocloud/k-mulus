package cbcb.kmulus;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import cbcb.kmulus.db.cluster.ClusterPresenceVectors;
import cbcb.kmulus.db.processing.GenerateSequencePresenceVectors;
import cbcb.kmulus.db.processing.PrepareClusteringOutput;
import cbcb.kmulus.db.processing.UnionClusterPresenceVectors;
import cbcb.kmulus.db.processing.WriteClusterSequencesToHDFS;
import cbcb.kmulus.db.processing.WriteSequencesToCluster;

import com.google.common.collect.ImmutableMap;

/**
 * The pipeline for generating clustered database partitions from a single database.
 * 
 * @author CH Albach
 */
public class PartitionDatabase {
	
	private static final String USAGE = 
		"PartitionDatabase DATABASE_SEQS OUTPUT_DIR NUM_SEQ NUM_CLUSTERS\n" +
		"\t[[START]:[STOP]] [KMER_LEN]\n" +
		"\tSTART and STOP indicate which range of steps should be run:\n" +
		"\t{ r-repeat mask | t-transform to PV | c-cluster\n" +
		"\t  p-prepare output | w-write partitions | u-union centers }";
	
	/* Final output directories. */
	private static final String PARTITIONS_SUFFIX = "partitions";
	private static final String CENTERS_SUFFIX = "centers";
	
	/* Intermediate output directories. */
	private static final String TEMP_SUFFIX = "temp";
	private static final String GENERATE_SUFFIX = "gen";
	private static final String CLUSTER_SUFFIX = "cluster";
	private static final String PREP_SUFFIX = "prep";
	
	private static final String DEFAULT_KMER_LEN = "3";
	
	private static final String STEP_DELIM = ":";
	private enum PipeStep {REPEAT_MASK, TRANSFORM_PV, CLUSTER, PREP, WRITE_PARTITIONS};
	private static final Map<Character, PipeStep> stepMap = ImmutableMap.<Character, PipeStep>builder()
			.put('r', PipeStep.REPEAT_MASK)
			.put('t', PipeStep.TRANSFORM_PV)
			.put('c', PipeStep.CLUSTER)
			.put('p', PipeStep.PREP)
			.put('w', PipeStep.WRITE_PARTITIONS)
			.put('u', PipeStep.UNION_CENTERS).build();
		
	
	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println(USAGE);
			return;
		}
		
		String dbInput = args[0];
		String tempOut = args[1] + Path.SEPARATOR + TEMP_SUFFIX;
		String finalOut = args[1];
		String numSeq = args[2];
		String numClusters = args[3];
		String kmerLen = args.length > 4 ? args[4] : DEFAULT_KMER_LEN;
		PipeStep start = PipeStep.REPEAT_MASK;
		PipeStep end = PipeStep.UNION_CENTERS;
		
		if (args.length > 5) {
			if (!args[5].contains(STEP_DELIM)) {
				System.err.println(USAGE);
				return;
			}
			String[] chunks = args[5].split(STEP_DELIM);
			String startStr = chunks[0];
			if (startStr.length() > 0) {
				start = stepMap.get(startStr.charAt(0));
			}
			
			if (chunks.length > 1) {
				String endStr = chunks[1];
				if (endStr.length() > 0) {
					end = stepMap.get(endStr.charAt(0));
				}
			}
			
			if (start == null || end == null) {
				System.err.println(USAGE);
				return;
			}
		}
		// Verify that the inputs are of the correct type.
		Integer.parseInt(numSeq);
		Integer.parseInt(numClusters);
		Integer.parseInt(kmerLen);
		
		try {
			/* Define all intermediate and final output directories. */
			String pvOut = tempOut + Path.SEPARATOR + GENERATE_SUFFIX;
			String clusterOut = tempOut + Path.SEPARATOR + CLUSTER_SUFFIX + Path.SEPARATOR + ClusterPresenceVectors.FINAL_DIR;
			String prepClusterOut = tempOut + Path.SEPARATOR + PREP_SUFFIX;
			String partitionsOut = finalOut + Path.SEPARATOR + PARTITIONS_SUFFIX;
			String centersOut = finalOut + Path.SEPARATOR + CENTERS_SUFFIX;


			switch (start) {
			case REPEAT_MASK:
				// Delete the output directories.
				Configuration conf = new Configuration();
				FileSystem.get(conf).delete(new Path(tempOut), true);
				FileSystem.get(conf).delete(new Path(finalOut), true);

				// TODO(calbach): Repeat masking.
				if (end == PipeStep.REPEAT_MASK) {
					break;
				}

				// Transform the database sequences into PresenceVectors.
			case TRANSFORM_PV:

				int result = ToolRunner.run(
						new GenerateSequencePresenceVectors(),
						new String[]{dbInput, pvOut, kmerLen});

				if (result < 0) {
					System.err.println(GenerateSequencePresenceVectors.class.getName() + " failed.");
					System.exit(result);
				}
				if (end == PipeStep.TRANSFORM_PV) {
					break;
				}

				// Cluster the PresenceVectors.
			case CLUSTER:
				clusterOut = tempOut + Path.SEPARATOR + CLUSTER_SUFFIX;
				int runIter = 0;
				do {
					result = ToolRunner.run(new Configuration(),
							new ClusterPresenceVectors(runIter),
							new String[]{pvOut, clusterOut, numSeq, numClusters, kmerLen});

					runIter++;
				} while (result == ClusterPresenceVectors.CODE_LOOP);

				if (result == ClusterPresenceVectors.CODE_CONVERGED) {
					result = ToolRunner.run(new Configuration(), 
							new ClusterPresenceVectors(),
							new String[]{pvOut, clusterOut, numSeq, numClusters, kmerLen});
				}

				if (result < 0) {
					System.err.println(ClusterPresenceVectors.class.getName() + " failed.");
					System.exit(result);
				}
				clusterOut += Path.SEPARATOR + ClusterPresenceVectors.FINAL_DIR;

				if (end == PipeStep.CLUSTER) {
					break;
				}

				// Reformat the clustering output for partitioning.
			case PREP:

				result = ToolRunner.run(
						new PrepareClusteringOutput(),
						new String[]{clusterOut, prepClusterOut});

				if (result < 0) {
					System.err.println(PrepareClusteringOutput.class.getName() + " failed.");
					System.exit(result);
				}
				if (end == PipeStep.PREP) {
					break;
				}

				// Generate the database partitions.
			case WRITE_PARTITIONS:
				result = ToolRunner.run(
						new WriteSequencesToCluster(),
						new String[]{prepClusterOut, dbInput, partitionsOut, tempOut + Path.SEPARATOR + "part", numClusters});

				if (result < 0) {
					System.err.println(WriteSequencesToCluster.class.getName() + " failed.");
					System.exit(result);
				}
				
				result = ToolRunner.run(
						new WriteClusterSequencesToHDFS(),
						new String[]{tempOut + Path.SEPARATOR + "part", partitionsOut, tempOut + Path.SEPARATOR + "null", numClusters});

				if (result < 0) {
					System.err.println(WriteClusterSequencesToHDFS.class.getName() + " failed.");
					System.exit(result);
				}
				
				if (end == PipeStep.WRITE_PARTITIONS) {
					break;
				}

				// Generate a union vector as the center for each cluster.
			case UNION_CENTERS:
				result = ToolRunner.run(
						new UnionClusterPresenceVectors(),
						new String[]{clusterOut, centersOut});

				if (result < 0) {
					System.err.println(UnionClusterPresenceVectors.class.getName() + " failed.");
					System.exit(result);
				}
				if (end == PipeStep.UNION_CENTERS) {
					break;
				}

			default:
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Pipeline failed.");
		}
	}
}
