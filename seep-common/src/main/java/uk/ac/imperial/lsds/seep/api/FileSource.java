package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.comm.serialization.Serializer;
import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;


public class FileSource {

	// Used only internally to create a logical operator that can connect
	private static QueryBuilder qb = new QueryBuilder();
	// Used only internally to connect to other LogicalOperators
	private static LogicalOperator lo;
	
	private static String relativePath = null;
	private static Serializer serializer = null;
	
	private String filePath;
	private SerializerType serdeType;
	
	public FileSource(String relativePath, SerializerType s){
		this.filePath = filePath;
		this.serdeType = serdeType;
	}
	
	private static class FileSourceImpl implements SeepTask{
		@Override
		public void setUp() {		}
		@Override
		public void processData(ITuple data, API api) {		}
		@Override
		public void processDataGroup(ITuple dataBatch, API api) {		}
		@Override
		public void close() {		}
	}
	
	static{
		// Initialize the logical operator
		lo = qb.newStatelessSource(new FileSourceImpl(), -1);
	}
	
	public static void connectTo(LogicalOperator downstreamOperator, int streamId, Schema schema){
		if(relativePath == null || serializer == null){
			throw new InvalidInitializationException("Invalid FileSource initialization. Set up relativePath and serializer");
		}
		lo.connectTo(downstreamOperator, streamId, schema, ConnectionType.ONE_AT_A_TIME, DataOrigin.FILE);
	}
	
	public static void connectTo(LogicalOperator downstreamOperator, int streamId, Schema schema, ConnectionType conType){
		if(relativePath == null || serializer == null){
			throw new InvalidInitializationException("Invalid FileSource initialization. Set up relativePath and serializer");
		}
		lo.connectTo(downstreamOperator, streamId, schema, conType, DataOrigin.FILE);
	}
	
	public static void setRelativePath(String relativePath){
		FileSource.relativePath = relativePath;
	}
	
	public static void setSerializer(Serializer serializer){
		FileSource.serializer = serializer;
	}

}
