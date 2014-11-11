package uk.ac.imperial.lsds.seep.comm.protocol;

@Deprecated
public class StartRuntimeCommand implements CommandType {
	
	public StartRuntimeCommand(){}

	@Override
	public short type() {
		return ProtocolAPI.STARTRUNTIME.type();
	}
	
}
