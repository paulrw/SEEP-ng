package uk.ac.imperial.lsds.seep.api.data;

/**
 * Tuple is a class that contains data in a byte[] form and an accompanying schema. Data will be deserialized only when
 * is explicitly needed, which will reduce overhead in many cases. All overhead is then pushed to the application, not part
 * of the system.
 * @author ra
 */

public class ITuple {

	private final Schema schema;
	private byte[] data;
	
	public ITuple(Schema schema){
		this.schema = schema;
		// create machinery with offsets to a byte[] where particular fields would be
	}
	
	public void setData(byte[] data){
		this.data = data;
	}
	
	public byte[] getData(){
		return data;
	}
	
}