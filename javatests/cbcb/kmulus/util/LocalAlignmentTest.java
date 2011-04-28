package cbcb.kmulus.util;

import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

/** Tests for {@link LocalAlignment}. */
public class LocalAlignmentTest extends TestCase {
	
	/** Test for {@link LocalAlignment#getLocalAlignment(Text, Text, int, int, int, int)}. */
	public void testGetLocalAlignment() {
		String a = ">1 asdfasdfasdf";
		String b = ">2 asdfzzzzasdfqiwjkre";
		
		Text aText = new Text(a);
		Text bText = new Text(b);

		LocalAlignment localHadoop = LocalAlignment.getLocalAlignment(aText, bText, 10, -5, -5, -2);
		SerialLocalAlignment localSerial = SerialLocalAlignment.align(a, b, 10, -5, -5, -2);
		assertEquals(localSerial.getScore(), localHadoop.getScore());
		assertEquals(localSerial.getDistance(), localHadoop.getDistance());
		assertNotSame(0, localHadoop.getScore());
		
		localHadoop = LocalAlignment.getLocalAlignment(bText, aText, 10, -5, -5, -2);
		assertEquals(localSerial.getScore(), localHadoop.getScore());
		assertEquals(localSerial.getDistance(), localHadoop.getDistance());
		assertNotSame(0, localHadoop.getScore());
		System.out.println(localHadoop.getScore() + " " + localHadoop.getDistance());
	}
	
}
