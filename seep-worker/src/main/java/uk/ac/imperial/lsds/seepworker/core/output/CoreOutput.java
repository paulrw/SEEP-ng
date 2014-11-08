package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class CoreOutput {
	
	private Map<Writer, OutputAdapter> outputAdaptersWithWriters;
		
	public CoreOutput(Map<Writer, OutputAdapter> outputAdaptersWithWriters){
		this.outputAdaptersWithWriters = outputAdaptersWithWriters;
	}
	
	public Map<Writer, OutputAdapter> getOutputAdaptersWithWriters(){
		return outputAdaptersWithWriters;
	}
	
	public List<OutputAdapter> getOutputAdapters(){
		List<OutputAdapter> l = new ArrayList<>();
		l.addAll(outputAdaptersWithWriters.values());
		return l;
	}
	
}
