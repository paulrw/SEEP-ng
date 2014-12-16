package uk.ac.imperial.lsds.seepworker.core.input;

import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import uk.ac.imperial.lsds.seep.api.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class FileDataStream implements InputAdapter {

	final private short RETURN_TYPE = InputAdapterReturnType.ONE.ofType();
	final private boolean REQUIRES_NETWORK = false;
	final private boolean REQUIRES_FILE = true;
	
	final private int streamId;
	private ITuple iTuple;
	
	private InputBuffer buffer;
	private BlockingQueue<byte[]> queue;
	private int queueSize;
	
	public FileDataStream(WorkerConfig wc, int streamId, Schema expectedSchema, List<UpstreamConnection> upc){
		this.streamId = streamId;
		this.iTuple = new ITuple(expectedSchema);
		this.queueSize = wc.getInt(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH);
		this.queue = new ArrayBlockingQueue<byte[]>(queueSize);
		this.buffer = new InputBuffer(wc.getInt(WorkerConfig.RECEIVE_APP_BUFFER_SIZE));
	}
	
	@Override
	public int getStreamId() {
		return streamId;
	}

	@Override
	public short returnType() {
		return RETURN_TYPE;
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
	public void readFrom(ReadableByteChannel channel, int id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void pushData(byte[] data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void pushData(List<byte[]> data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ITuple pullDataItem(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ITuple pullDataItems(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

}
