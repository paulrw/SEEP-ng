package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import uk.ac.imperial.lsds.seep.api.Operator;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class NetworkDataStream implements InputAdapter{

	final private short RETURN_TYPE = InputAdapterReturnType.ONE.ofType();
	final private boolean REQUIRES_NETWORK = true;
	final private boolean REQUIRES_FILE = false;
	
	private InputBuffer buffer;
	private BlockingQueue<byte[]> queue;
	private int queueSize;
	
	final private int streamId;
	private ITuple iTuple;
	private List<Operator> ops;
	
	public NetworkDataStream(WorkerConfig wc, int streamId, Schema expectedSchema, List<Operator> ops) {
		this.streamId = streamId;
		this.ops = ops;
		this.queueSize = wc.getInt(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH);
		this.queue = new ArrayBlockingQueue<byte[]>(queueSize);
		this.iTuple = new ITuple(expectedSchema);
		this.buffer = new InputBuffer(wc.getInt(WorkerConfig.RECEIVE_APP_BUFFER_SIZE));
	}
	
	@Override
	public int getStreamId(){
		return streamId;
	}
	
	@Override
	public InputBuffer getInputBuffer(){
		return buffer;
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public ITuple pullDataItem(int timeout) {
		byte[] data = null;
		try {
			if(timeout > 0){
				// Need to poll rather than take due to the implementation of some ProcessingEngines
				data = queue.poll(timeout, TimeUnit.MILLISECONDS);
			} else{
				data = queue.take();
			}
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		// In case poll was used, and it timeouts
		if(data == null){
			return null;
		}
		iTuple.setData(data);
		iTuple.setStreamId(streamId);
		return iTuple;
	}

	@Override
	public ITuple pullDataItems(int timeout) {
		// TODO batching oriented, or window, or barrier, etc...
		return null;
	}
}
