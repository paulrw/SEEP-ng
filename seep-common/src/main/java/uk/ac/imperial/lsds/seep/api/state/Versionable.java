package uk.ac.imperial.lsds.seep.api.state;

public interface Versionable {

	public void enterSnapshotMode();
	public void reconcile();
	
}
