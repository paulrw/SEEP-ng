package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import uk.ac.imperial.lsds.seep.api.Operator;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class NetworkDataStream implements InputAdapter{

	final private short RETURN_TYPE = InputAdapterReturnType.ONE.ofType();
	final private boolean REQUIRES_NETWORK = true;
	final private boolean REQUIRES_FILE = false;
	
	private BlockingQueue<byte[]> queue;
	private int queueSize;
	
	final private int streamId;

	private Schema expectedSchema;
	private ITuple iTuple;
	
	private List<Operator> ops;
	
	public NetworkDataStream(WorkerConfig wc, int streamId, Schema expectedSchema, List<Operator> ops) {
		this.streamId = streamId;
		this.expectedSchema = expectedSchema;
		this.ops = ops;
		this.queueSize = wc.getInt(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH);
		this.queue = new ArrayBlockingQueue<byte[]>(queueSize);
		this.iTuple = new ITuple(expectedSchema);
	}
	
	@Override
	public boolean requiresNetwork() {
		return REQUIRES_NETWORK;
	}

	@Override
	public boolean requiresFile() {
		return REQUIRES_FILE;
	}

	@Override
	public short rType() {
		return RETURN_TYPE;
	}
	
	@Override
	public void pushData(byte[] data){
		try {
			queue.put(data);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public ITuple pullDataItem() {
		byte[] data = null;
		try {
			data = queue.take();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		iTuple.setData(data);
		return iTuple;
	}

	@Override
	public ITuple pullDataItems() {
		// TODO batching oriented, or window, or barrier, etc...
		return null;
	}
}
