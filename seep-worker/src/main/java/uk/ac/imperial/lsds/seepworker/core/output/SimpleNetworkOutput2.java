package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seepworker.comm.EventAPI;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;


public class SimpleNetworkOutput2 implements OutputAdapter2 {

	final private boolean requiresNetworkWorker = true;
	final private boolean requiresFileWorker = false;
	
	private int streamId;
	private Router router;
	private Map<Integer, OutputBuffer2> outputBuffers;
	private OutputBuffer2 ob;
	private EventAPI eAPI;
	
	public SimpleNetworkOutput2(int streamId, Router router, Map<Integer, OutputBuffer2> outputBuffers){
		this.router = router;
		this.streamId = streamId;
		this.outputBuffers = outputBuffers;
		if(outputBuffers.size() == 1){
			ob = outputBuffers.values().iterator().next();
		}
	}
	
	@Override
	public void setEventAPI(EventAPI eAPI) {
		this.eAPI = eAPI;
	}
	
	@Override
	public boolean requiresNetwork() {
		return requiresNetworkWorker;
	}
	
	@Override
	public boolean requiresFile() {
		return requiresFileWorker;
	}
	
	@Override
	public Map<Integer, OutputBuffer2> getOutputBuffers(){
		return outputBuffers;
	}

	@Override
	public int getStreamId() {
		return streamId;
	}

	@Override
	public void send(byte[] o) {
		//OutputBuffer ob = outputBuffers.get(0);
		boolean canSend = ob.write(o); // unique outputBuffer
		if(canSend){
			eAPI.readyForWrite(ob.id());
		}
	}

	@Override
	public void sendAll(byte[] o) {
		List<Integer> ids = new ArrayList<>();
		for(OutputBuffer2 ob : outputBuffers.values()){
			boolean canSend = ob.write(o);
			if(canSend) 
				ids.add(ob.id());
		}
		if(ids.size() > 0){
			eAPI.readyForWrite(ids);
		}
	}

	@Override
	public void sendKey(byte[] o, int key) {
		OutputBuffer2 ob = null; // temp hack
		boolean canSend =  ob.write(o);
		if(canSend){
			eAPI.readyForWrite(ob.id());
		}
	}

	/**
	 * TODO: fix these non-defined things
	 */
	
	@Override
	public void sendKey(byte[] o, String key) {
		// NON DEFINED
	}

	@Override
	public void sendToStreamId(int streamId, byte[] o) {
		// NON DEFINED
	}

	@Override
	public void sendToAllInStreamId(int streamId, byte[] o) {
		// NON DEFINED
	}

	@Override
	public void sendStreamidKey(int streamId, byte[] o, int key) {
		// NON DEFINED
	}

	@Override
	public void sendStreamidKey(int streamId, byte[] o, String key) {
		// NON DEFINED
	}

	@Override
	public void send_index(int index, byte[] o) {
		// TODO Auto-generated method stub
		// careful i guess
	}

	@Override
	public void send_opid(int opId, byte[] o) {
		// TODO Auto-generated method stub
		// careful again
	}
	
}
