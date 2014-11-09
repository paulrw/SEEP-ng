package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.Operator;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;

public class NetworkDataStream implements InputAdapter{

	final private short RETURN_TYPE = InputAdapterReturnType.ONE.ofType();
	final private boolean REQUIRES_NETWORK = true;
	final private boolean REQUIRES_FILE = false;
	
	final private int streamId;

	private Schema expectedSchema;
	private List<Operator> ops;
	
	public NetworkDataStream(int streamId, Schema expectedSchema, List<Operator> ops) {
		this.streamId = streamId;
		this.expectedSchema = expectedSchema;
		this.ops = ops;
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
	public ITuple pullDataItem() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ITuple pullDataItems() {
		// TODO Auto-generated method stub
		return null;
	}
}
