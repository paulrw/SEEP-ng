package uk.ac.imperial.lsds.seep.api.data;

public class TupleInfo {
	public static final int CONTROL_OVERHEAD = 1;
	public static final int NUM_TUPLES_BATCH_OVERHEAD = 4;
	public static final int BATCH_SIZE_OVERHEAD = 4;
	public static final int TUPLE_SIZE_OVERHEAD = 4;
	
	public static final int PER_BATCH_OVERHEAD_SIZE = CONTROL_OVERHEAD 
											+ NUM_TUPLES_BATCH_OVERHEAD 
											+ BATCH_SIZE_OVERHEAD; // control byte + batch_tuples
	
	public static final int BATCH_SIZE_OFFSET = CONTROL_OVERHEAD + NUM_TUPLES_BATCH_OVERHEAD;
}
