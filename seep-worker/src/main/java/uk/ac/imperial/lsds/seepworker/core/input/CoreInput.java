package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.List;
import java.util.Map;


public class CoreInput {
	
	private List<InputAdapter> inputAdapters;
	private Map<Integer, InputAdapter> iapMap;
	
	public CoreInput(List<InputAdapter> inputAdapters){
		this.inputAdapters = inputAdapters;
		
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
