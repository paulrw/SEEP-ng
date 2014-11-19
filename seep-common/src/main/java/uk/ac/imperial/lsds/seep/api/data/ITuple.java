package uk.ac.imperial.lsds.seep.api.data;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Tuple is a class that contains data in a byte[] form and an accompanying schema. Data will be deserialized only when
 * is explicitly needed, which will reduce overhead in many cases. All overhead is then pushed to the application, not part
 * of the system.
 * @author ra
 */

public class ITuple {

	private final Schema schema;
	private Map<String, Integer> mapFieldToOffset;
	private ByteBuffer wrapper;
	private byte[] data;
	
	public ITuple(Schema schema){
		this.schema = schema;
		mapFieldToOffset = new HashMap<>();
		if(! schema.isVariableSize()){
			// This only happens once
			this.populateOffsets();
		}
	}
	
	public void setData(byte[] data){
		this.data = data;
		// greedily populate offsets for lazy deserialisation
		if(schema.isVariableSize()){
			this.populateOffsets();
		}
		wrapper = ByteBuffer.wrap(data);
	}
	
	public byte[] getData(){
		return data;
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
	
	/** Consider moving these fields to a different interface to not expose the rest to users? **/
	
	public byte getByte(String fieldName){
		if(! schema.hasField(fieldName)){
			// TODO: error no field
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)) {
			// TODO: does not type check
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.get();
	}
	
//	public boolean getBoolean(String fieldName){
//			
//		return false;
//	}
	
//	public char getChar(String fieldName){
//
//		return 0;
//	}
	
	public short getShort(String fieldName){
		if(! schema.hasField(fieldName)){
			// TODO: error no field
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)) {
			// TODO: does not type check
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getShort();
	}
	
	public int getInt(String fieldName){
		if(! schema.hasField(fieldName)){
			// TODO: error no field
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)) {
			// TODO: does not type check
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getInt();
	}
	
	public long getLong(String fieldName){
		if(! schema.hasField(fieldName)){
			// TODO: error no field
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)) {
			// TODO: does not type check
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		wrapper.position(offset);
		return wrapper.getLong();
	}
	
	public String getString(String fieldName){
//		if(! schema.hasField(fieldName)){
//			// TODO: error no field
//		}
//		else if(! schema.typeCheck(fieldName, Type.BYTE)) {
//			// TODO: does not type check
//		}
//		
//		int offset = mapFieldToOffset.get(fieldName);
//		wrapper.position(offset);
//		return wrapper.get();
		return null;
	}
	
}