package uk.ac.imperial.lsds.seepworker.core.output;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;


public class SimpleNetworkOutput implements OutputAdapter {

	final private boolean requiresNetworkWorker = true;
	
	private int streamId;
	private Router router;
	private Map<Integer, OutputBuffer> outputBuffers;
	
	private Selector s;
	
	private Map<Integer, SelectionKey> mapIdToSelKey;
	
	public SimpleNetworkOutput(int streamId, Router router, Map<Integer, OutputBuffer> outputBuffers, Selector s){
		this.router = router;
		this.streamId = streamId;
		this.outputBuffers = outputBuffers;
		this.s = s;
		this.mapIdToSelKey = new HashMap<>();
		for(SelectionKey sk : s.selectedKeys()){
			OutputBuffer ob = (OutputBuffer)sk.attachment();
			mapIdToSelKey.put(ob.id(), sk);
		}
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
			SelectionKey sk = mapIdToSelKey.get(ob.id());
			int interestOps = sk.interestOps() | SelectionKey.OP_WRITE;
			sk.interestOps(interestOps);
			s.wakeup();
		}
	}

	@Override
	public void sendAll(OTuple o) {
		for(OutputBuffer ob : outputBuffers.values()){
			ob.write(o.getData());
		}
	}

	@Override
	public void sendKey(OTuple o, int key) {
		// TODO;
	}

	@Override
	public void sendKey(OTuple o, String key) {
		// TODO Auto-generated method stub
		// same
	}

	@Override
	public void sendStreamid(int streamId, OTuple o) {
		// TODO Auto-generated method stub
		// non defined
	}

	@Override
	public void sendStreamidAll(int streamId, OTuple o) {
		// TODO Auto-generated method stub
		// non defined
	}

	@Override
	public void sendStreamidKey(int streamId, OTuple o, int key) {
		// TODO Auto-generated method stub
		// non defined
	}

	@Override
	public void sendStreamidKey(int streamId, OTuple o, String key) {
		// TODO Auto-generated method stub
		// non defined
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
	
	
	class Sender implements Runnable {

		@Override
		public void run() {
			
			// check the output buffers, whatever that is and send downstream to the configured socket
			// this would require a wait-notify mechanism, otherwise this guy will be working like crazy
			
		}	
	}
	
}
