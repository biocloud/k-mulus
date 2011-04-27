package cbcb.kmulus.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/** 
 * Converts a FASTA file to the simple FASTA format, which is one sequence per line.
 * 
 * @author cmhill
 */
public class FastaToSimpleFasta {
	public final static int TRIPLET_LENGTH = 3;
	public static int NUMBER_OF_CORES = 114;
	
	public static void main(String[] args) {	
		File sequenceDir = new File(args[0]);
			
		String outputFileName = args[1] + ".0.simple_faa";
		String metaFileName = args[1] + ".sma";
		
		long total_sequences = new Long(args[2]);
		long seq_per_file = (long) Math.ceil(((double) total_sequences) / NUMBER_OF_CORES);
		
		System.out.println("Sequences per file: " + seq_per_file);
		
		long counter = 0;
		
		try {
			File ouputFile = new File(outputFileName);
			BufferedWriter ow = new BufferedWriter(new FileWriter(ouputFile));
	
			// TODO(calbach): Write a mapping of internal_id -> external_id.
			File metaFile = new File(metaFileName);
			BufferedWriter mw = new BufferedWriter(new FileWriter(metaFile));
				
			long count_per_file = 0;
			
			File[] listOfFiles = sequenceDir.listFiles();
			
			for (File seqFile : listOfFiles) {
			ParseFasta pf = new ParseFasta(seqFile);
			String[] record = null;
			
			while ((record = pf.getRecord()) != null) {
				//StringBuilder translation = new StringBuilder("");
				
				String sequence = record[1];
				
				// Forward Sequence
				ow.write(">" + counter + " ");
				ow.write(sequence);
				ow.write("\n");
				++counter;
				
				// Check to see if we need to write to a new sequence file.
				count_per_file += 1;
				if (count_per_file > seq_per_file) {
					ow.close();
					
					ouputFile = new File(args[1] + "." + counter + ".simple_faa");
					ow = new BufferedWriter(new FileWriter(ouputFile));
					
					count_per_file = 0;
				}
				
			}
			
			}
			ow.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
