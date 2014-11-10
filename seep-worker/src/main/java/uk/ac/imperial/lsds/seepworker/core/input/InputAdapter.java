package uk.ac.imperial.lsds.seepworker.core.input;

import uk.ac.imperial.lsds.seep.api.data.ITuple;


public interface InputAdapter {

	public short rType();
	
	public boolean requiresNetwork();
	public boolean requiresFile();
	
	public ITuple pullDataItem();
	public ITuple pullDataItems();
	
}