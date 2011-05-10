package cbcb.kmulus.blast;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 * BlastReducer receives the cluster id, followed by the list of sequences
 * that share a k-mer in common with that cluster.
 */
public class BlastReducer extends Reducer<LongWritable, Text, Text, Text> {

	private static final Logger LOG = Logger.getLogger(BlastReducer.class);
	
	private String blastDBDir = null;
	
	protected static String BLAST_PROG = "./blastall -p blastp ";
	
	protected void setup(Context context) throws IOException, InterruptedException {
		blastDBDir = context.getConfiguration().get(Blast.BLAST_DATABASES, "blastdbs");
	}
	
	/**
	 * Run the command on the local hadoop node.
	 * 
	 * @param command List of arguments.
	 * @return the stdout and stderr.
	 */
	protected String runCommand(List<String> command) {
		StringBuilder programOutput = new StringBuilder("");
		LOG.info(command.toString() + "\n");
		
		try {
			//Process p = Runtime.getRuntime().exec(command);
			ProcessBuilder builder = new ProcessBuilder(command);
			builder.redirectErrorStream(true);
			Process p = builder.start();

			InputStream stdin = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(stdin);
			BufferedReader br = new BufferedReader(isr);

			String line = null;
			System.out.println("<OUTPUT>");
			
			while ((line = br.readLine()) != null) {
				programOutput.append(line + "\n");
			}
			System.out.println("</OUTPUT>");
			
			LOG.info("Starting process.\n");
			int exitVal = p.waitFor();
			LOG.info("Finished process.\n");
		} catch (Exception e) {
			LOG.info(e.getMessage());
			e.printStackTrace();
			programOutput.append(e.getMessage());
		}
		
		return programOutput.toString();
	}
	
	
	/**
	 * Write all sequences to a tmp file: [clusterid].fasta
	 */
	private boolean writeSequencesToFile(Iterable<Text> values, String filename) {
		try {
			FileWriter fstream = new FileWriter(filename);
			BufferedWriter out = new BufferedWriter(fstream);
			
			// Write each sequence out to file.
			String[] headerAndSequence = null;
			for (Text value : values) {
				headerAndSequence = value.toString().split(" ");
				out.write(headerAndSequence[0] + "\n" + headerAndSequence[1] + "\n");
			}
			
			out.close();
			fstream.close();
			return true;
		} catch (IOException e) {
			return false;
		}
	}
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// What blast database should we use?
		String blastDB = key.toString();
		
		// Write sequences to a temp file to use for blast.
		String blastSequencesFileName = blastDB + ".fasta";
		
		if (!writeSequencesToFile(values, blastSequencesFileName)) {
			// Throw error.
			return;
		}
		
		String blastCommand = BLAST_PROG + "-d " + blastDBDir + "/dbs/" + blastDB + "_db/" + blastDB +
				" -i " + blastDB + ".fasta";

		StringBuilder results = new StringBuilder("");
		List<String> arguments = new ArrayList<String>();
		
		// Blast command has to be broken down into arguments.  
		// because of ProcessBuilder. TODO(cmhill): revert to old way.
		arguments.add("./blastall");
		arguments.add("-p");
		arguments.add("blastp");
		arguments.add("-d");
		arguments.add(blastDBDir + "/dbs/" + blastDB + "_db/" + blastDB);
		arguments.add("-m8"); // Change to customizable blast output!
		arguments.add("-i");
		arguments.add(blastDB + ".fasta");
		
		LOG.info("Running command: " + blastCommand );
		results.append("Running command: " + blastCommand + "\n" + runCommand(arguments));
		
		// Delete the tmp sequence file.
		File tempSequenceFile = new File(blastSequencesFileName);
		if (tempSequenceFile.exists())
			tempSequenceFile.delete();
		
		/* Sample commands
		results.append("Running command 'ls -l': \n" + runCommand("ls -l"));
		results.append("Running command './blastall': \n" + runCommand("./blastall"));
		results.append("Running command 'ls -l mycache': \n" + runCommand("ls -l mycache/data"));
		results.append("Running command 'cat mycache/1/1.fasta': \n" + runCommand("cat mycache/data/1/1.fasta"));
		*/
		
		context.write(new Text(key.toString()), new Text(results.toString()));
	}
}
