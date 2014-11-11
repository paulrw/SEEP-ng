package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seepworker.comm.EventAPI;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;


public class SimpleNetworkOutput implements OutputAdapter {

	final private boolean requiresNetworkWorker = true;
	
	private int streamId;
	private Router router;
	private Map<Integer, OutputBuffer> outputBuffers;
	private EventAPI eAPI;
	
	public SimpleNetworkOutput(int streamId, Router router, Map<Integer, OutputBuffer> outputBuffers){
		this.router = router;
		this.streamId = streamId;
		this.outputBuffers = outputBuffers;
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
			eAPI.readyForWrite(ob.id());
		}
	}

	@Override
	public void sendAll(OTuple o) {
		List<Integer> ids = new ArrayList<>();
		for(OutputBuffer ob : outputBuffers.values()){
			boolean canSend = ob.write(o.getData());
			if(canSend) 
				ids.add(ob.id());
		}
		if(ids.size() > 0){
			eAPI.readyForWrite(ids);
		}
	}

	@Override
	public void sendKey(OTuple o, int key) {
		OutputBuffer ob = router.route(outputBuffers, key);
		boolean canSend =  ob.write(o.getData());
		if(canSend){
			eAPI.readyForWrite(ob.id());
		}
	}

	/**
	 * TODO: fix these non-defined things
	 */
	
	@Override
	public void sendKey(OTuple o, String key) {
		// NON DEFINED
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
