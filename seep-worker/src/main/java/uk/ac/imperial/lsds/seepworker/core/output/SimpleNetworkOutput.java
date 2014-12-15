package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seepworker.comm.EventAPI;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;


public class SimpleNetworkOutput implements OutputAdapter {

	final private boolean requiresNetworkWorker = true;
	final private boolean requiresFileWorker = false;
	
	private int streamId;
	private Router router;
	private Map<Integer, OutputBuffer> outputBuffers;
	private OutputBuffer ob;
	private EventAPI eAPI;
	
	public SimpleNetworkOutput(int streamId, Router router, Map<Integer, OutputBuffer> outputBuffers){
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
	public Map<Integer, OutputBuffer> getOutputBuffers(){
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
		for(OutputBuffer ob : outputBuffers.values()){
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
		OutputBuffer ob = router.route(outputBuffers, key);
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
