package uk.ac.imperial.lsds.seepworker.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.errors.DoYouKnowWhatYouAreDoingException;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.core.output.OutputAdapter;
import uk.ac.imperial.lsds.seepworker.core.output.routing.NotEnoughRoutingInformation;

public class Collector implements API {

	private final boolean SEND_NOT_DEFINED;
	
	private OutputAdapter outputAdapter;
	private List<OutputAdapter> outputAdapters;
	private Map<Integer, OutputAdapter> streamIdToOutputAdapter;
	
	public Collector(List<OutputAdapter> outputAdapters){
		if(outputAdapters.size() > 1)
			SEND_NOT_DEFINED = true;
		else {
			SEND_NOT_DEFINED = false;
			// Configure the unique outputadapter in this collector to avoid one lookup
			outputAdapter = outputAdapters.get(0);
		}
		this.outputAdapters = outputAdapters;
		this.streamIdToOutputAdapter = createMap(outputAdapters);
	}
	
	private Map<Integer, OutputAdapter> createMap(List<OutputAdapter> outputAdapters){
		Map<Integer, OutputAdapter> tr = new HashMap<>();
		for(OutputAdapter o : outputAdapters){
			tr.put(o.getStreamId(), o);
		}
		return tr;
	}
	
	@Override
	public void send(OTuple o) {
		if(SEND_NOT_DEFINED){
			throw new NotEnoughRoutingInformation("There are more than one streamId downstream; you must specify where "
					+ "you are sending to");
		}
		outputAdapter.send(o);
	}

	@Override
	public void sendAll(OTuple o) {
		if(SEND_NOT_DEFINED){
			throw new NotEnoughRoutingInformation("There are more than one streamId downstream; you must specify where "
					+ "you are sending to");
		}
		outputAdapter.sendAll(o);
	}

	@Override
	public void sendKey(OTuple o, int key) {
		outputAdapter.sendKey(o, key);
	}

	@Override
	public void sendKey(OTuple o, String key) {
		outputAdapter.sendKey(o, key);
	}

	@Override
	public void sendStreamid(int streamId, OTuple o) {
		streamIdToOutputAdapter.get(streamId).send(o);
	}

	@Override
	public void sendStreamidAll(int streamId, OTuple o) {
		streamIdToOutputAdapter.get(streamId).sendAll(o);
	}

	@Override
	public void sendStreamidKey(int streamId, OTuple o, int key) {
		streamIdToOutputAdapter.get(streamId).sendKey(o, key);
	}

	@Override
	public void sendStreamidKey(int streamId, OTuple o, String key) {
		int numericKey = Utils.hashString(key);
		streamIdToOutputAdapter.get(streamId).sendKey(o, numericKey);
	}

	@Override
	public void send_index(int index, OTuple o) {
		throw new DoYouKnowWhatYouAreDoingException("This is mostly a debugging method, you should not be playing with the"
				+ "underlying communication directly otherwise...");
	}

	@Override
	public void send_opid(int opId, OTuple o) {
		throw new DoYouKnowWhatYouAreDoingException("You seem to know too much about the topology of this dataflow...");
	}
	
}
