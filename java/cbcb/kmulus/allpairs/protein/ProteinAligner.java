package cbcb.kmulus.allpairs.protein;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import cbcb.kmulus.allpairs.comparison.AllPairsMapper;
import cbcb.kmulus.allpairs.comparison.AllPairsReducer;
import cbcb.kmulus.allpairs.comparison.ExhaustiveUniqueGrouper;
import cbcb.kmulus.allpairs.comparison.PairSplit;
import cbcb.kmulus.allpairs.comparison.PrimeRot;
import cbcb.kmulus.util.Alignment;
import cbcb.kmulus.util.AlphabetMap;
import cbcb.kmulus.util.Biology;
import cbcb.kmulus.util.GlobalAlignment;
import cbcb.kmulus.util.LocalAlignment;


/**
 * Hadoop program which aligns all pairs of protein against one another.  A dynamic programming
 * comparison function is used: either for {@link LocalAlignment} or {@link GlobalAlignment}.
 */
public class ProteinAligner extends Configured implements Tool {

	private static final String SFA_META_FILE_EXT = ".sfm";
	private static final String SFA_FILE_EXT = ".sfa";

	private static final String SFA_DIR = "simple";

	private static final String[] inputExts = {".fa", ".fasta", ".fsa_aa"};

	/**
	 * Identifies the different implementations of {@link ExhaustiveUniqueGrouper}s supported by 
	 * {@link ProteinAligner}.
	 */
	enum EugType {PRIME_ROT, PAIR_SPLIT}

	/*Configuration attribute names for grouping data.*/
	static final String NUM_SEQ_ATTR = "ns";
	static final String EUG_ATTR = "eug";
	
	/*Configuration attribute names for alignment parameters.*/
	static final String COMPARE_ATTR = "la";
	static final String ALPHABET_ATTR = "al";
	static final String MATCH_SCORE_ATTR = "ms";
	static final String MISMATCH_SCORE_ATTR = "mm";
	static final String GAP_SCORE_ATTR = "gap";
	static final String GAP_EXT_SCORE_ATTR = "gex";
	static final String KMER_LEN_ATTR = "kmr";
	
	/** Identifies the way in which two proteins should be compared. */
	enum CompareType {KMER, LOCAL, GLOBAL}
	
	/*Default alignment parameters.*/
	private static final CompareType DEF_COMPARE = CompareType.KMER;
	private static final String DEF_ALPHABET = Biology.alphabetStrA20;
	private static final int DEF_MATCH_SCORE = 10;
	private static final int DEF_MISMATCH_SCORE = -5;
	private static final int DEF_GAP_SCORE = -5;
	private static final int DEF_GAP_EXT_SCORE = -2;
	private static final int DEF_KMER_LEN = 3;
	
	/**
	 * The mapper in {@link ProteinAligner} does minimal work.
	 * 
	 * 1) Parses the id from a sequence.
	 * 2) Passes the id to an ExhaustiveUniqueGrouper.
	 * 3) Uses the resulting group id as the output key, and the sequence as the output value.
	 */
	public static class Map extends AllPairsMapper<LongWritable,Text> {

		private AlphabetMap alphabetMap;
		private ExhaustiveUniqueGrouper eug;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			
			long numSeq = conf.getLong(NUM_SEQ_ATTR, 0);
			int eugID = conf.getInt(EUG_ATTR, EugType.PRIME_ROT.ordinal());
			alphabetMap = new AlphabetMap(conf.getStrings(ALPHABET_ATTR, DEF_ALPHABET)[0]);
			
			// To allow for all 6 open reading frames, will generate 6 fold sequences.
			numSeq *= 6;
			
			if (numSeq < 0) {
				eug = null;
			} else {
				
				if (eugID ==  EugType.PRIME_ROT.ordinal()) {
					eug = PrimeRot.generatePrimeRot(numSeq);
				
				} else if (eugID == EugType.PRIME_ROT.ordinal()) {
					
					try {
						eug = new PairSplit(numSeq);
					} catch (IOException e) {
						eug = null;
					}

				} else {
					eug = PrimeRot.generatePrimeRot(numSeq);
				}
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			long id = parseId(key, value);
			String seq = value.toString();
			seq = seq.substring(seq.indexOf(' ') + 1);
			
			// Generate the protein products of all 3 forward reading frames.
			for (int offset = 0; offset < 3; offset++) {
				StringBuilder protein = new StringBuilder();
				for (int i = offset; i < seq.length() - 2; i+=3) {
					protein.append(alphabetMap.get(seq.substring(i, i + 3)));
				}
				
				// Increment the id by one and map it as usual.
				String proteinFasta = ">" + (id++) + " " + protein.toString();
				super.map(key, new Text(proteinFasta), context);
			}
			
			// Generate the protein products of all 3 reverse reading frames.
			for (int offset = 0; offset < 3; offset++) {
				StringBuilder protein = new StringBuilder();
				for (int i = seq.length() - offset; i > 2; i-=3) {
					protein.append(alphabetMap.revCompGet(seq.substring(i - 3, i)));
				}
				
				// Increment the id by one and map it as usual.
				String proteinFasta = ">" + (id++) + " " + protein.toString();
				super.map(key, new Text(proteinFasta), context);;
			}
		}

		@Override
		protected long parseId(LongWritable key, Text value) throws IOException {
			long seqId = parseSeqId(value);

			if (seqId > eug.getNumItems()) {
				throw new IOException("Parsed a sequence id which is too high. Got " + 
						seqId + ", should be less than " + eug.getNumItems() + 
						".\nTroublesome sequence:" + value);
			}

			return seqId;
		}

		@Override
		protected ExhaustiveUniqueGrouper getGrouper() throws IOException {
			if (eug == null) {
				throw new IOException("Grouper was not properly initialized.");
			}
			return eug;
		}
	}

	/**
	 * The reducer performs the bulk of the work.
	 * 
	 * For each pair of sequences:
	 * 1) Parse the ids of both sequences.
	 * 2) Align the two sequences to one another.
	 * 3) Combine the ids to form the output key, use the alignment score as the output value.
	 */
	public static class Reduce extends AllPairsReducer<Text, LongWritable> {

		private CompareType compareType;
		private int matchScore, mismatchScore, gapScore, gapExtScore;
		private int kmerLength;
		
		@Override
		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();
			
			/* Initialize the parameters for the alignment. */
			compareType = 
				CompareType.values()[config.getInt(COMPARE_ATTR, DEF_COMPARE.ordinal())];
			matchScore = config.getInt(MATCH_SCORE_ATTR, DEF_MATCH_SCORE);
			mismatchScore = config.getInt(MISMATCH_SCORE_ATTR, DEF_MISMATCH_SCORE);
			gapScore = config.getInt(GAP_SCORE_ATTR, DEF_GAP_SCORE);
			gapExtScore = config.getInt(GAP_EXT_SCORE_ATTR, DEF_GAP_EXT_SCORE);
			kmerLength = config.getInt(KMER_LEN_ATTR, DEF_KMER_LEN);
		}
		
		@Override
		protected LongWritable compareItems(Text a, Text b) {
			Alignment alignment;
			
			switch (compareType) {
			case LOCAL:
				alignment = LocalAlignment.getLocalAlignment(
					a, b, matchScore, mismatchScore, gapScore, gapExtScore);
				break;
				
			case GLOBAL:
				alignment = GlobalAlignment.getGlobalAlignment(
					a, b, matchScore, mismatchScore, gapScore);
				break;
				
			case KMER:
				// Drop through.
			default:
				alignment = KmerDistance.getKmerAlignment(a, b, kmerLength);
			}

			return new LongWritable(alignment.getDistance());
		}

		@Override
		protected long parseId(Text value) throws IOException {
			return parseSeqId(value);
		}

		@Override
		protected Text copyValue(Text original) {
			return new Text(original);
		}
	}


	/** Handles runtime flags for the {@link ProteinAligner}. */
	private static class ProteinAlignerOptions {

		private static final String FLAG_MARKER = "-";

		private boolean preprocess = true;
		private boolean convertOnly = false;

		private String baseName;
		private Path fastaPath;
		private Path inputPath;
		private Path outputPath;
		
		private int numReduceTasks = 114;

		public ProteinAlignerOptions(String[] args) throws IOException {

			int i = 0;

			if (args[i].startsWith(FLAG_MARKER)) {
				int toIncrement = 1;

				for (Character c : args[i].substring(1).toCharArray()) {

					switch(c) {

					/*ONLY convert to simple FASTA*/
					case 'c':
					case 'C':
						convertOnly = true;
						break;

					case 's':
					case 'S':
						preprocess = false;
						break;

						/*Specify a base name for this run.*/
					case 'b':
					case 'B':
						baseName = args[i + 1];
						toIncrement++;
						break;
						
					case 'r':
					case 'R':
						numReduceTasks = Integer.parseInt(args[i + 1]);
						toIncrement++;
						break;
						
					default:
						throw new IOException("Unrecognized flag: " + c + ".");
					}

					i+= toIncrement;
				}
			}

			if (i >= args.length - 1)
				throw new IOException("An input and output path are required.");

			fastaPath = new Path(args[i]);
			inputPath = new Path(args[i] + Path.SEPARATOR + SFA_DIR);
			outputPath = new Path(args[i+1]);

			if (baseName == null || baseName.equals(""))
				baseName = ProteinAligner.computeDefaultBaseName(fastaPath);
		}

		public boolean isPreprocess() {
			return preprocess;
		}

		public boolean isConvertOnly() {
			return convertOnly;
		}

		public String getBaseName() {
			return baseName;
		}

		public Path getFastaPath() {
			return fastaPath;
		}

		public Path getInputPath() {
			return inputPath;
		}

		public Path getOutputPath() {
			return outputPath;
		}

		public int getNumReduceTasks() {
			return numReduceTasks;
		}
	}

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		ProteinAlignerOptions opts = new ProteinAlignerOptions(args);

		if (opts.isPreprocess()) {
			toSimpleFasta(opts.getFastaPath(), opts.getInputPath(), opts.getBaseName());
			System.out.println("Finished preprocessing successfully.");
		}

		if (!opts.isConvertOnly()) {

			Job job = new Job(getConf());
			job.setJarByClass(ProteinAligner.class);
			job.setJobName("proteinAligner");

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setNumReduceTasks(opts.getNumReduceTasks());
			
			FileInputFormat.setInputPaths(job, opts.getInputPath());
			FileOutputFormat.setOutputPath(job, opts.getOutputPath());

			setNumSequences(job, opts.getFastaPath(), opts.getBaseName());

			return job.waitForCompletion(true) ? 0 : 1;
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws IOException {
		int result = 1;
		
		try {
			result = ToolRunner.run(new ProteinAligner(), args);
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Job failed.");
		}
		System.exit(result);
	}

	/**
	 * Assume the text has been pre-processed in the following form:
	 * 		>123 GATTACA...
	 * 
	 * Parse the sequence Id.
	 */
	public static long parseSeqId(Text value) throws IOException {
		
		/*Read the sequence number*/
		int start = value.find(">");
		int end = value.find(" ") > 0 ? value.find(" ") : Integer.MAX_VALUE;
		end = Math.min(end, value.find("\t") > 0 ? value.find("\t") : Integer.MAX_VALUE);
		end = Math.min(end, value.find("\n") > 0 ? value.find("\n") : Integer.MAX_VALUE);
		String seqId = "";

		if (start < 0 || end == Integer.MAX_VALUE) {
			throw new IOException("Couldn't find " + (start < 0 ? "'>'" : "whitespace") + ".");
		}
		
		try {
			seqId = Text.decode(value.getBytes(), start + 1, end - 1);
			return Long.parseLong(seqId);

		} catch(NumberFormatException e) {
			throw new IOException("Couldn't parse seqID (" + seqId + ") from: " + value);
		} catch (CharacterCodingException e1) {
			throw new IOException("Encoding error parsing seqID from: " + value);
		}
	}	

	/** Retrieves the number of sequences in the input SFA file, from the associated metafile. */
	public static void setNumSequences(Job job, Path fastaPath, String baseName)
			throws IOException {
		FileSystem hdfs = FileSystem.get(job.getConfiguration());

		/*Find the associated meta-file*/
		Path metaPath = new Path(fastaPath + Path.SEPARATOR + baseName + SFA_META_FILE_EXT);

		if (!hdfs.exists(metaPath))
			throw new IOException("Expected to find meta-file at location " + metaPath + ".");

		/*Read the number of sequences*/
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(metaPath)));
		Long n = Long.parseLong(br.readLine().trim());
		br.close();
		
		/* Multiply by 6 for the 6 open reading frames. */
		n *= 6;
		
		/*Set the corresponding attribute in the JobConf*/
		job.getConfiguration().set(NUM_SEQ_ATTR, n.toString());
	}

	/**
	 * Converts a FASTA file to the 'simple FASTA format', which is one sequence
	 * per line with internal sequence IDs.  Also generates a meta-file which
	 * maps iids -> external ids and keeps a count of the total number of 
	 * sequences in the FASTA file.
	 * 
	 * If the meta-file has been generated more recently than the FASTA file
	 * has been modified, do nothing.
	 */
	public static void toSimpleFasta(Path srcPath, String baseName) throws IOException {
		Path defaultDestPath = new Path(srcPath + Path.SEPARATOR + SFA_DIR);
		toSimpleFasta(srcPath, defaultDestPath, baseName);
	}

	/**
	 * Converts a FASTA file to the 'simple FASTA format', which is one sequence
	 * per line with internal sequence IDs.  Also generates a meta-file which
	 * maps iids -> external ids and keeps a count of the total number of 
	 * sequences in the FASTA file.
	 * 
	 * If the meta-file has been generated more recently than the FASTA file
	 * has been modified, do nothing.
	 * 
	 * @param srcPath - Location of the directory containing the input files.
	 * @param destPath - Location of the directory where the simple FASTA file
	 * 				should land.
	 */
	public static void toSimpleFasta(Path srcPath, Path destPath, String baseName) throws IOException {

		FileSystem hdfs = FileSystem.get(new Configuration());

		if (!hdfs.exists(srcPath)) {
			throw new IOException(srcPath + " does not exist.");
		}

		for (int i = 0; i < inputExts.length; i++) {

			if (baseName.endsWith(inputExts[i])) {
				baseName = baseName.substring(0, baseName.length() - inputExts[i].length());
				break;
			}
		}

		/* If the directory doesn't exist, try to make it. */
		if (!hdfs.exists(destPath) && !hdfs.mkdirs(destPath)) {
			throw new IOException("Couldn't create directory at " + destPath + ".");

		} else if (!hdfs.getFileStatus(destPath).isDir()) {
			throw new IOException(destPath + " was a file. Expected a directory.");
		}


		/* Look for the associated simple FASTA file. */
		Path sfaPath = new Path(destPath + Path.SEPARATOR + baseName + SFA_FILE_EXT);

		if (!hdfs.exists(sfaPath) && !hdfs.createNewFile(sfaPath)) {
			throw new IOException(
					"Couldn't write " + SFA_FILE_EXT + " file to " + destPath + ".");
		}

		/* Look for the associated meta-file. */
		Path metaPath = new Path(srcPath + Path.SEPARATOR + baseName + SFA_META_FILE_EXT); 
		boolean needRewrite = true;

		if (hdfs.exists(metaPath)) {
			/*TODO
			long metaModifyTime = hdfs.getFileStatus(metaPath).getModificationTime();

			/*Check if the sfa file has been updated more recently.

			/*Check if the meta-file was updated more recently than any of the FASTA files

			if (metaStatus.getModificationTime() > srcDir.lastModified())
				needRewrite = false;
			 */

		} else if (!hdfs.createNewFile(metaPath)) {
			throw new IOException("Couldn't write meta-file to " + destPath + ".");
		}

		/* Rewrite the FASTA file to 'simple FASTA format'. */
		if (needRewrite) {

			BufferedWriter sfaOut = new BufferedWriter(new OutputStreamWriter(hdfs.create(sfaPath)));
			List<String> seqIds = new ArrayList<String>();
			String line;
			int iid = 0;

			for (FileStatus srcFileStatus : hdfs.listStatus(srcPath)) {
				if (srcFileStatus.isDir() || srcFileStatus.getPath().toString().endsWith(SFA_META_FILE_EXT))
					continue;

				BufferedReader faIn = new BufferedReader(new InputStreamReader(hdfs.open(srcFileStatus.getPath())));
				boolean firstLine = true;

				while((line = faIn.readLine()) != null) {

					/* If this line is a sequence id, convert to an internal id. */
					if (line.startsWith(">")) {	
						if (firstLine) {
							firstLine = false;
						} else {
							sfaOut.append("\n");
						}

						sfaOut.append(">" + iid + " ");
						int firstSpace = line.indexOf(" ");
						firstSpace = firstSpace > 0 ? firstSpace : Integer.MAX_VALUE;
						
						int firstTab = line.indexOf("\t");
						firstTab = firstTab > 0 ? firstTab : Integer.MAX_VALUE;
						
						int firstWhiteSpace = Math.min(firstSpace, firstTab);
						if (firstWhiteSpace < Integer.MAX_VALUE) {
							seqIds.add(iid + " " + line.substring(1, firstWhiteSpace));
							sfaOut.append(line.substring(firstWhiteSpace + 1));
							
						} else {
							seqIds.add(iid + " " + line.substring(1));
						}
						iid++;

						/* If its not a sequence id, write it on the same line. */	
					} else {
						sfaOut.append(line);
					}
				}
				faIn.close();
			}
			sfaOut.close();
			
			BufferedWriter metaOut = new BufferedWriter(new OutputStreamWriter(hdfs.create(metaPath)));

			/* Write the total number of sequences at the beginning of the file. */
			metaOut.append(iid + "\n");

			/* Write the iid + seqId pairs. */
			for (String seqId : seqIds) {
				metaOut.append(seqId + "\n");
			}

			metaOut.close();
		}

		hdfs.close();
	}

	public static String computeDefaultBaseName(Path fastaPath) throws IOException {
		FileSystem hdfs = FileSystem.get(new Configuration());
		List<String> fileNames = new ArrayList<String>();

		/* Grab all of the file names in the given directory of FASTA files. */
		for (FileStatus fs : hdfs.listStatus(fastaPath)) {
			if (!fs.isDir()) {
				fileNames.add(fs.getPath().getName());
			}
		}
		hdfs.close();

		if (fileNames.isEmpty())
			throw new IOException("Expected input files in directory " + fastaPath + ".");

		/* Sort them for consistency between runs. */
		Collections.sort(fileNames);
		String baseName = fileNames.get(0);

		/* Trim any fasta file extensions. */
		for (String ext : inputExts) {
			if (baseName.endsWith(ext)) {
				baseName = baseName.substring(0, baseName.length() - ext.length());
				break;
			}
		}
		return baseName;
	}
}
