package uk.ac.imperial.lsds.seep.api.data;

import java.nio.ByteBuffer;
import java.util.Map;

public abstract class TempTuple {

	private final Schema schema;
	private Map<String, Integer> mapFieldToOffset;
	private ByteBuffer wrapper;
	private byte[] data;
	
	protected TempTuple(Schema schema){
		this.schema = schema;
	}
	
	private void populateOffsets(){
		Type[] fields = schema.fields();
		String[] names = schema.names();
		int offset = 0;
		for(int i = 0; i < fields.length; i++){
			Type t = fields[i];
			mapFieldToOffset.put(names[i], offset);
			if(! t.isVariableSize()){
				// if not variable we just get the size of the Type
				offset = offset + t.sizeOf(null);
			}
			else {
				// if variable we need to read the size from the current offset
				ByteBuffer temp = ByteBuffer.wrap(data);
				temp.position(offset);
				int size = temp.getInt();
				offset = offset + size;
			}
		}
	}
	
}
