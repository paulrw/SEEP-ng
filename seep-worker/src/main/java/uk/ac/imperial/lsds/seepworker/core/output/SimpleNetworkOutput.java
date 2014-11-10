package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.Map;

import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;


public class SimpleNetworkOutput implements OutputAdapter {

	final private boolean requiresNetworkWorker = true;
	
	private int streamId;
	private Router router;
	private Map<Integer, OutputBuffer> outputBuffers;
	
	public SimpleNetworkOutput(int streamId, Router router, Map<Integer, OutputBuffer> outputBuffers){
		this.router = router;
		this.streamId = streamId;
		this.outputBuffers = outputBuffers;
	}
	
	@Override
	public boolean requiresNetwork() {
		return requiresNetworkWorker;
	}
	
	@Override
	public Map<Integer, OutputBuffer> getOutputBuffers(){
		return outputBuffers;
	}

	@Override
	public int getStreamId() {
		return streamId;
	}

	@Override
	public void send(OTuple o) {
		OutputBuffer ob = outputBuffers.get(0);
		boolean canSend = ob.write(o.getData());
		if(canSend){
//			SelectionKey sk = mapIdToSelKey.get(ob.id());
//			int interestOps = sk.interestOps() | SelectionKey.OP_WRITE;
//			sk.interestOps(interestOps);
//			s.wakeup();
		}
	}

	@Override
	public void sendAll(OTuple o) {
		boolean canSend = false;
		for(OutputBuffer ob : outputBuffers.values()){
			boolean readyToSend = ob.write(o.getData());
			canSend = canSend | readyToSend;
		}
		if(canSend){
			
		}
	}

	@Override
	public void sendKey(OTuple o, int key) {
		OutputBuffer obuf = router.route(outputBuffers, key);
		boolean canSend =  obuf.write(o.getData());
		if(canSend){
			
		}
	}

	@Override
	public void sendKey(OTuple o, String key) {
		int hashedKey = Utils.hashString(key);
		this.sendKey(o, hashedKey);
	}

	@Override
	public void sendStreamid(int streamId, OTuple o) {
		// NON DEFINED
	}

	@Override
	public void sendStreamidAll(int streamId, OTuple o) {
		// NON DEFINED
	}

	@Override
	public void sendStreamidKey(int streamId, OTuple o, int key) {
		// NON DEFINED
	}

	@Override
	public void sendStreamidKey(int streamId, OTuple o, String key) {
		// NON DEFINED
	}

	@Override
	public void send_index(int index, OTuple o) {
		// TODO Auto-generated method stub
		// careful i guess
	}

	@Override
	public void send_opid(int opId, OTuple o) {
		// TODO Auto-generated method stub
		// careful again
	}
	
}
