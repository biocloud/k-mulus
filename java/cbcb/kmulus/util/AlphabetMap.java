package cbcb.kmulus.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Supports compressed amino acid alphabets in which several amino acids are represented as a 
 * single letter.  Along with the obvious mapping of amino acid to alphabet letter, also allows for
 * direct lookup from the DNA triplet to the alphabet letter.
 */
public class AlphabetMap {

	private final Map<Character, Character> aaToAlphabet;
	private final Map<String, Character> dnaToAlphabet;
	private final Map<String, Character> revCompDnaToAlphabet;

	/**
	 * Takes an alphabet as a string in the form '(A B C) (D E) (F) (G H)', where A B and C are
	 * a class to be compressed as a single letter.
	 * 
	 * @param classStr the formatted string with parenthesis denoting a class
	 */
	public AlphabetMap(String classStr) throws IOException {
		this(parseClasses(classStr));
 	}
	
	public AlphabetMap(char[][] classes) throws IOException {
		this(classes, Biology.defaultTable);
	}

	public AlphabetMap(char[][] classes, AATable table) throws IOException {
		Preconditions.checkNotNull(table);
		Preconditions.checkNotNull(classes);

		aaToAlphabet = Maps.newHashMap();
		dnaToAlphabet = Maps.newHashMap();
		revCompDnaToAlphabet = Maps.newHashMap();
		initAlphabet(classes, table);
	}

	public char get(char aa) throws IOException {
		if (!aaToAlphabet.containsKey(aa)) {
			throw new IOException("Amino acid '" + aa + "' was not present in the alphabet.");
		}
		return aaToAlphabet.get(aa);
	}
	
	public char get(String triplet) throws IOException {
		if (!dnaToAlphabet.containsKey(triplet)) {
			throw new IOException("Triplet '" + triplet + "' was not present in the alphabet.");
		}
		return dnaToAlphabet.get(triplet);
	}
	
	public char revCompGet(String triplet) {
		return revCompDnaToAlphabet.get(triplet);
	}
	
	@VisibleForTesting
	void initAlphabet(char[][] classes, AATable table) throws IOException {
		char alphabetIndex = 'A';

		for (char[] set : classes) {
			for (char c : set) {
				if (aaToAlphabet.containsKey(c)) {
					throw new IOException("Amino acid: " + c + " was present at least twice in" +
					"the given alphabet.");
				}

				aaToAlphabet.put(c, alphabetIndex);
			}
			alphabetIndex++;
		}

		for (String triplet : Biology.TRIPLETS) {
			Character aa = table.get(triplet);

			if (aa == Biology.TERMINATOR) {
				dnaToAlphabet.put(triplet, Biology.TERMINATOR);
				revCompDnaToAlphabet.put(Biology.revComp(triplet), Biology.TERMINATOR);
				
			} else {
				Character alphabetLetter = aaToAlphabet.get(aa);

				if (alphabetLetter == null) {
					throw new IOException("Amino acid: '" + aa + "' was not present in" +
					" the given alphabet.");
				}
				dnaToAlphabet.put(triplet, alphabetLetter);
				revCompDnaToAlphabet.put(Biology.revComp(triplet), alphabetLetter);
			}
		}
	}
	
	@VisibleForTesting
	static char[][] parseClasses(String classStr) throws IOException {
		List<String> groups = Lists.newArrayList();
		
		int nextIndex = 0;
		while ((nextIndex = classStr.indexOf('(')) >= 0) {
			int endIndex = classStr.indexOf(')');
			if (endIndex < nextIndex) {
				throw new IOException("Invalid syntax at '" + classStr + "'.");
			}
			groups.add(classStr.substring(nextIndex + 1, endIndex));
			classStr = classStr.substring(endIndex + 1);
		}
		
		char[][] classes = new char[groups.size()][];
		for (int i = 0; i < groups.size(); i++) {
			StringTokenizer tokenizer = new StringTokenizer(groups.get(i).trim());
			
			classes[i] = new char[tokenizer.countTokens()];
			for (int j = 0; j < classes[i].length; j++) {
				String letter = tokenizer.nextToken();
				if (letter.length() != 1) {
					throw new IOException("Syntax error at: '" + letter + "'.");
				
				} else {
					classes[i][j] = letter.charAt(0);
				}
			}
		}
		
		return classes;
	}
}
