package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CoreInput {
	
	final private static Logger LOG = LoggerFactory.getLogger(CoreInput.class);
	
	private List<InputAdapter> inputAdapters;
	private Map<Integer, InputAdapter> iapMap;
	
	public CoreInput(List<InputAdapter> inputAdapters){
		this.inputAdapters = inputAdapters;
		iapMap = new HashMap<>();
		for(InputAdapter ia : inputAdapters){
			iapMap.put(ia.getStreamId(), ia);
		}
		LOG.info("Configured CoreInput with {} inputAdapters", inputAdapters.size());
	}
	
	public List<InputAdapter> getInputAdapters(){
		return inputAdapters;
	}
	
	public boolean requiresConfiguringNetworkWorker(){
		for(InputAdapter ia : inputAdapters){
			if(ia.requiresNetwork())
				return true;
		}
		return false;
	}
	
	public Map<Integer, InputAdapter> getInputAdapterProvider(){
		return iapMap;
	}
	
}
