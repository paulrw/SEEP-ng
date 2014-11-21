package uk.ac.imperial.lsds.seep.api.data;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class OTuple {

	private Schema schema;
	private Object[] values;
	private Map<String, Integer> mapFieldToOffset;
	private final int fixedSchemaSize;
	
	private byte[] data;
	
	public OTuple(Schema schema){
		this.schema = schema;
		this.values = new Object[schema.fields().length];
		mapFieldToOffset = new HashMap<>();
		if(! schema.isVariableSize()){
			// This only happens once
			this.populateOffsets();
			this.fixedSchemaSize = this.calculateSizeFromSchema();
		}
		else {
			this.fixedSchemaSize = -1; // variable size, so it needs to be computed per tuple
		}
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
	
	public byte[] getData(){
		return data;
	}
	
	public byte[] create(Schema schema, String[] fields, Object[] vs) {
		OTuple o = new OTuple(schema);
		if(fields.length != vs.length){
			// TODO: error here
			System.exit(0);
		}
		if(fields.length != schema.fields().length){
			// TODO: error here
			System.exit(0);
		}
		Object[] values = new Object[vs.length];
		for(int i = 0; i < fields.length; i++){
			Object toTypeCheck = vs[i];
			if(! schema.typeCheck(fields[i], toTypeCheck)){
				// TODO: error here
				System.exit(0);
			}
			else{
				values[i] = toTypeCheck;
			}
		}
		o.values = values;
		this.populateOffsets();
		return o.getBytes();
	}
	
	public OTuple setByte(String fieldName, byte b){
		if(! schema.hasField(fieldName)){
			// TODO:
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)){
			// TODO:
		}
		int position = schema.getFieldPosition(fieldName);
		values[position] = b;
		return this;
	}
	
	public OTuple setShort(String fieldName, short s){
		if(! schema.hasField(fieldName)){
			// TODO:
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)){
			// TODO:
		}
		int position = schema.getFieldPosition(fieldName);
		values[position] = s;
		return this;
	}
	
	public OTuple setInt(String fieldName, int i){
		if(! schema.hasField(fieldName)){
			// TODO:
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)){
			// TODO:
		}
		int position = schema.getFieldPosition(fieldName);
		values[position] = i;
		return this;
	}
	
	public OTuple setLong(String fieldName, long l){
		if(! schema.hasField(fieldName)){
			// TODO:
		}
		if(! schema.typeCheck(fieldName, Type.BYTE)){
			// TODO:
		}
		int position = schema.getFieldPosition(fieldName);
		values[position] = l;
		return this;
	}
	
	public OTuple setString(String fieldName, String str){
//		int offset = mapFieldToOffset.get(fieldName);
//		wrapper.position(offset);
//		wrapper.put(b);
		return this;
	}
	
	public byte[] getBytes(){
		int requiredSize = this.fixedSchemaSize;
		if(schema.isVariableSize()){
			requiredSize = calculateSizeFromSchema(); 
		}
		data = new byte[requiredSize];
		ByteBuffer wrapper = ByteBuffer.wrap(data);
		for(int i = 0; i < values.length; i++){
			Type t = schema.fields()[i];
			t.write(wrapper, values[i]);
		}
		return data;
	}
	
	private int calculateSizeFromSchema(){
		int size = 0;
		for(int i = 0; i < schema.fields().length; i++){
			Type t = schema.fields()[i];
			size = size + t.sizeOf(values[i]);
		}
		return size;
	}
	
}
