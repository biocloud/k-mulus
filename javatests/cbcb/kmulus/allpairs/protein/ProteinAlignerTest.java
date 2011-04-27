package cbcb.kmulus.allpairs.protein;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.IOException;
import java.util.Set;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.easymock.IAnswer;

import cbcb.kmulus.allpairs.protein.ProteinAligner;
import cbcb.kmulus.util.Biology;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/** Tests for {@link ProteinAligner}. */
public class ProteinAlignerTest extends TestCase {

	/** Test for {@link ProteinAligner#parseSeqId(Text)}. */
	public void testParseSeqId() throws Exception {
		long actualId = ProteinAligner.parseSeqId(new Text(">0 ATATAGTAGAT"));
		assertEquals(0L, actualId);
		
		actualId = ProteinAligner.parseSeqId(new Text(">99  ATATATATATAT"));
		assertEquals(99L, actualId);
		
		actualId = ProteinAligner.parseSeqId(new Text(">103\tATAGATAG"));
		assertEquals(103L, actualId);
		
		actualId = ProteinAligner.parseSeqId(new Text(">107\nATAGATAG"));
		assertEquals(107L, actualId);
		
		actualId = ProteinAligner.parseSeqId(new Text(">1337\n\t \tATATATA"));
		assertEquals(1337L, actualId);
	}
	
	/** Test for {@link ProteinAligner#parseSeqId(Text)} on invalid input. */
	public void testParseSeqId_invalid() throws Exception {
		try {
			ProteinAligner.parseSeqId(new Text("> ATGTGTG"));
			fail();
			
		} catch (IOException e) {
			// Expected.
		}
		
		try {
			ProteinAligner.parseSeqId(new Text("> 123 ATGTGTG"));
			fail();
			
		} catch (IOException e) {
			// Expected.
		}
		
		try {
			ProteinAligner.parseSeqId(new Text(">123ATGTGTG"));
			fail();
			
		} catch (IOException e) {
			// Expected.
		}
		
		try {
			ProteinAligner.parseSeqId(new Text(">1F43 ATGTGTG"));
			fail();
			
		} catch (IOException e) {
			// Expected.
		}
	}
	
	/** Test for {@link ProteinAligner.Map#map(LongWritable, Text, Mapper.Context)}. */
	public void testMap() throws Exception {
		ProteinAligner.Map map = new ProteinAligner.Map();
		
		// Mock out the context and configuration.
		Mapper<LongWritable, Text, LongWritable, Text>.Context context = 
			(Mapper<LongWritable, Text, LongWritable, Text>.Context) createMock(Mapper.Context.class);
		Configuration conf = createMock(Configuration.class);
		
		// Expect calls to get parameters from the configuration.
		expect(conf.getLong(eq(ProteinAligner.NUM_SEQ_ATTR), anyLong())).andReturn(1L);
		expect(conf.getInt(eq(ProteinAligner.EUG_ATTR), anyInt())).andAnswer(
				new IAnswer<Integer>() {
					@Override
					public Integer answer() throws Throwable {
						// Return the default value (second argument in the call).
						return (Integer) getCurrentArguments()[1];
					}
				});
		expect(conf.getStrings(ProteinAligner.ALPHABET_ATTR, Biology.alphabetStrA20)).andAnswer(
				new IAnswer<String[]>() {
					@Override
					public String[] answer() throws Throwable {
						// Return the default value (var args from the call).
						return new String[] {(String) getCurrentArguments()[1]};
					}
				});
		expect(context.getConfiguration()).andReturn(conf);
		
		// Store all values as they are emitted by the Mapper.
		final Set<String> actualValues = Sets.newHashSet();
		context.write(isA(LongWritable.class), isA(Text.class));
		expectLastCall().andAnswer(new IAnswer<Void> () {

			@Override
			public Void answer() throws Throwable {
				// Take the written sequence and store it for later checks.
				String seq = getCurrentArguments()[1].toString();
				seq = seq.substring(seq.indexOf(' ') + 1);
				actualValues.add(seq);
				
				return null;
			}
			
		}).anyTimes();
		
		replay(context, conf);
		map.setup(context);
		map.map(new LongWritable(1L), new Text(">0 ACTTGACA"), context);
		
		verify(context, conf);
		
		// Check that all 6 open reading frames are generated.
		Set<String> expectedValues = ImmutableSet.of(
				"" + Biology.alphabetA20.get("ACT") + Biology.alphabetA20.get("TGA"),
				"" + Biology.alphabetA20.get("CTT") + Biology.alphabetA20.get("GAC"),
				"" + Biology.alphabetA20.get("TTG") + Biology.alphabetA20.get("ACA"),
				"" + Biology.alphabetA20.revCompGet("ACA") + Biology.alphabetA20.revCompGet("TTG"),
				"" + Biology.alphabetA20.revCompGet("GAC") + Biology.alphabetA20.revCompGet("CTT"),
				"" + Biology.alphabetA20.revCompGet("TGA") + Biology.alphabetA20.revCompGet("ACT"));
		Assert.assertEquals(expectedValues, actualValues);
	}
}
