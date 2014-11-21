package uk.ac.imperial.lsds.seep.api.data;

import static org.junit.Assert.*;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class OTupleTest {

	@Test
	public void test() {
		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		byte[] sData = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{666, 333333L});
		
		ITuple i = new ITuple(s);
		i.setData(sData);
		String tuple = i.toString();
		System.out.println(tuple);
		
		assertTrue(true);
	}

}
