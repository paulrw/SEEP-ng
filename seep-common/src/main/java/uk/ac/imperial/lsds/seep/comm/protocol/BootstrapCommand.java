package uk.ac.imperial.lsds.seep.comm.protocol;

public class BootstrapCommand implements CommandType {

	private String ip;
	private int port;
	
	// FIXME: include the data port, so that every worker can be configured in a dif one and the cluster knows
	
	public BootstrapCommand(){}
	
	public BootstrapCommand(String ip, int port){
		this.ip = ip;
		this.port = port;
	}
	
	@Override
	public short type() {
		return ProtocolAPI.BOOTSTRAP.type();
	}
	
	public String getIp(){
		return ip;
	}
	
	public int getPort(){
		return port;
	}

}
