package uk.ac.imperial.lsds.seep.api.state;

import java.util.Iterator;

public interface Streamable {

	public Iterator<? extends Object> makeStream();
	
}
