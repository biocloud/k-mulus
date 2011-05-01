package cbcb.kmulus.util;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/** Contains basic Biology information. */
public class Biology {

	public static final char[] NUCLEOTIDES = {'A', 'T', 'C', 'G'};
	public static final String[] TRIPLETS = {"AAA", "AAT", "AAC", "AAG", "ATA", "ATT", "ATC", "ATG",
		"ACA", "ACT", "ACC", "ACG", "AGA", "AGT", "AGC", "AGG", "TAA", "TAT", "TAC", "TAG", "TTA",
		"TTT", "TTC", "TTG", "TCA", "TCT", "TCC", "TCG", "TGA", "TGT", "TGC", "TGG", "CAA", "CAT",
		"CAC", "CAG", "CTA", "CTT", "CTC", "CTG", "CCA", "CCT", "CCC", "CCG", "CGA", "CGT", "CGC",
		"CGG", "GAA", "GAT", "GAC", "GAG", "GTA", "GTT", "GTC", "GTG", "GCA", "GCT", "GCC", "GCG",
		"GGA", "GGT", "GGC", "GGG"};

	public static final char[] AMINO_ACIDS = {'A', 'R', 'N', 'D', 'C', 'E', 'Q', 'G', 'H', 'I', 'L',
		'K', 'M', 'F', 'P', 'S', 'T', 'W', 'Y', 'V', '*'};
	public static final char TERMINATOR = '*';

	public static final AATable defaultTable = 
		new AATable(ImmutableMap.<String, Character>builder()
				.put("TTT", 'F').put("TTC", 'F').put("TTA", 'L').put("TTG", 'L')
				.put("TCT", 'S').put("TCC", 'S').put("TCA", 'S').put("TCG", 'S')
				.put("TAT", 'Y').put("TAC", 'Y').put("TAA", TERMINATOR).put("TAG", TERMINATOR)
				.put("TGT", 'C').put("TGC", 'C').put("TGA", TERMINATOR).put("TGG", 'W')
				.put("CTT", 'L').put("CTC", 'L').put("CTA", 'L').put("CTG", 'L')
				.put("CCT", 'P').put("CCC", 'P').put("CCA", 'P').put("CCG", 'P')
				.put("CAT", 'H').put("CAC", 'H').put("CAA", 'Q').put("CAG", 'Q')
				.put("CGT", 'R').put("CGC", 'R').put("CGA", 'R').put("CGG", 'R')
				.put("ATT", 'I').put("ATC", 'I').put("ATA", 'I').put("ATG", 'M')
				.put("ACT", 'T').put("ACC", 'T').put("ACA", 'T').put("ACG", 'T')
				.put("AAT", 'N').put("AAC", 'N').put("AAA", 'K').put("AAG", 'K')
				.put("AGT", 'S').put("AGC", 'S').put("AGA", 'R').put("AGG", 'R')
				.put("GTT", 'V').put("GTC", 'V').put("GTA", 'V').put("GTG", 'V')
				.put("GCT", 'A').put("GCC", 'A').put("GCA", 'A').put("GCG", 'A')
				.put("GAT", 'D').put("GAC", 'D').put("GAA", 'E').put("GAG", 'E')
				.put("GGT", 'G').put("GGC", 'G').put("GGA", 'G').put("GGG", 'G').build());

	public static final String alphabetStrA20 = 
		"(A) (R) (N) (D) (C) (E) (Q) (G) (H) (I) (L) (K) (M) (F) (P) (S) (T) (W) (Y) (V)";
	public static final AlphabetMap alphabetA20;

	static {
		try {
			alphabetA20 = new AlphabetMap(alphabetStrA20);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static final Map<Character, Integer> aaHashLookup;
	
	static {
		aaHashLookup = Maps.newHashMap();
		for (int i = 0; i < AMINO_ACIDS.length; i++) {
			aaHashLookup.put(Character.toLowerCase(AMINO_ACIDS[i]), i);
			aaHashLookup.put(Character.toUpperCase(AMINO_ACIDS[i]), i);
		}
	}
	
	/**
	 * Computes the hash index value for the given amino acid.
	 * 
	 * @param aa the amino acid character
	 * @return the hash index
	 */
	public static int getAAHash(char aa) {
		Integer hash = aaHashLookup.get(aa); 
		if (hash == null) {
			throw new RuntimeException("Residue '" + aa + "' not found.");
		}
		return hash;
	}
	
	/**
	 * Computes the hash index value for the given amino acid k-mer, which uses the original amino
	 * acid alphabet. 
	 * 
	 * @param kmer a kmer of amino acids
	 * @return the hash index
	 */
	public static int getAAKmerHash(String kmer) {
		int k = kmer.length();
		int posValue = 1;
		int hash = 0;
		for (int i = 0; i < k; i++) {
			hash += getAAHash(kmer.charAt(i)) * posValue;
			posValue *= k;
		}
		return hash;
	}
	
	/**
	 * Computes the hash index value for the given compressed alphabet k-mer.
	 * 
	 * @param kmer a kmer of amino acids
	 * @return the hash index
	 */
	public static int getCompressedHash(String kmer) {
		int k = kmer.length();
		int posValue = 1;
		int hash = 0;
		for (int i = 0; i < k; i++) {
			int index = kmer.charAt(i) - (Character.isUpperCase(kmer.charAt(i)) ? 'A' : 'a');
			hash += index * posValue;
			posValue *= k;
		}
		return hash;
	}
	
	/** Takes the reverse complement of the given DNA sequence. */
	public static String revComp(String dna) {
		StringBuilder revComp = new StringBuilder();
		for (int i = dna.length() - 1; i >= 0; i--) {
			switch (dna.charAt(i)) {
			case 'A':
			case 'a':
				revComp.append('T');
				break;

			case 'T':
			case 't':
				revComp.append('A');
				break;

			case 'C':
			case 'c':
				revComp.append('G');
				break;

			case 'G':
			case 'g':
				revComp.append('C');
				break;

			default:
				break;		
			}
		}
		
		return revComp.toString();
	}
}
