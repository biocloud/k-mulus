package cbcb.kmulus.util;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import junit.framework.TestCase;

/** Tests for {@link AlphabetMap}. */
public class AlphabetMapTest extends TestCase {

	private Map<Character, String> aaToTriplet;
	
	@Override
	public void setUp() {
		aaToTriplet = Maps.newHashMap();
		for (String triplet : Biology.TRIPLETS) {
			aaToTriplet.put(Biology.defaultTable.get(triplet), triplet);
		}
	}
	
	/** Test for {@link AlphabetMap#Alphabet(char[][])}. */
	public void testAlphabet() throws Exception {
		char[][] classes = {{'A'}, {'R', 'N', 'D'}, {'C', 'E', 'Q', 'G', 'H'}, {'I', 'L'},
				{'K', 'M'}, {'F', 'P', 'S', 'T'}, {'W', 'Y', 'V'}};
		AlphabetMap alphabet = new AlphabetMap(classes);
		
		assertEquals(alphabet.get(aaToTriplet.get('D')), alphabet.get(aaToTriplet.get('N')));
		assertEquals(alphabet.get(aaToTriplet.get('K')), alphabet.get(aaToTriplet.get('M')));
		assertEquals(alphabet.get(aaToTriplet.get('F')), alphabet.get(aaToTriplet.get('T')));
	
		assertNotSame(alphabet.get(aaToTriplet.get('A')), alphabet.get(aaToTriplet.get('R')));
		assertNotSame(alphabet.get(aaToTriplet.get('D')), alphabet.get(aaToTriplet.get('V')));
		assertNotSame(alphabet.get(aaToTriplet.get('K')), alphabet.get(aaToTriplet.get('I')));
	}
	
	/** Test for {@link AlphabetMap#Alphabet(char[][])} on invalid input. */
	public void testAlphabet_duplicate() {
		char[][] classes = {{'A'}, {'R', 'N', 'D'}, {'V', 'C', 'E', 'Q', 'G', 'H'}, {'I', 'L'},
				{'K', 'M'}, {'F', 'P', 'S', 'T'}, {'W', 'Y', 'V'}};
		
		try {
			new AlphabetMap(classes);
			fail();
			
		} catch (IOException e) {
			// Expected.
		}
	}
	
	/** Test for {@link AlphabetMap#Alphabet(char[][])} on invalid input. */
	public void testAlphabet_missing() {
		char[][] classes = {{'A'}, {'R', 'N', 'D'}, {'C', 'E', 'Q', 'G', 'H'}, {'I', 'L'},
				{'K', 'M'}, {'F', 'P', 'S', 'T'}, {'W', 'Y'}};
		
		try {
			new AlphabetMap(classes);
			fail();
			
		} catch (IOException e) {
			// Expected.
		}
	}

	/** Test for {@link AlphabetMap#parseClasses(String)}. */
	public void testParseClasses() throws Exception {
		String classStr = "(A) (R N D) (C E Q G H) (I L) (K M) (F P S T) (W Y)";
		char[][] expectedClasses = {{'A'}, {'R', 'N', 'D'}, {'C', 'E', 'Q', 'G', 'H'}, {'I', 'L'},
				{'K', 'M'}, {'F', 'P', 'S', 'T'}, {'W', 'Y'}};
		
		char[][] actualClasses = AlphabetMap.parseClasses(classStr);
		assertEquals(expectedClasses, actualClasses);
	}
	
	/** Test for {@link AlphabetMap#parseClasses(String)} with a single class. */
	public void testParseClasses_single() throws Exception {
		String classStr = "(A R N D C E Q G H I L K M F P S T W Y)";
		char[][] expectedClasses = {{'A', 'R', 'N', 'D', 'C', 'E', 'Q', 'G', 'H', 'I', 'L',
				'K', 'M', 'F', 'P', 'S', 'T', 'W', 'Y'}};
		
		char[][] actualClasses = AlphabetMap.parseClasses(classStr);
		assertEquals(expectedClasses, actualClasses);
	}
	
	/** Test for {@link AlphabetMap#parseClasses(String)} with the empty string. */
	public void testParseClasses_empty() throws Exception {
		String classStr = "";
		char[][] expectedClasses = {};

		char[][] actualClasses = AlphabetMap.parseClasses(classStr);
		assertEquals(expectedClasses, actualClasses);
	}

	/** Test for {@link AlphabetMap#parseClasses(String)} with invalid syntax. */
	public void testParseClasses_invalidSyntax() throws Exception {
		String classStr = "(A R N D C E Q (G H I L K M) F P S T W Y)";
		try {
			AlphabetMap.parseClasses(classStr);
			fail();
		
		} catch (IOException e) {
			// Expected.
		}
	}
	
	private static void assertEquals(char[][] expected, char[][] actual) {
		assertEquals(expected.length, actual.length);
		for (int i = 0; i < expected.length; i++) {
			
			assertEquals(expected[i].length, actual[i].length);
			for (int j = 0; j < expected[i].length; j++) {
				assertEquals(expected[i][j], actual[i][j]);
			}
		}
	}
}
