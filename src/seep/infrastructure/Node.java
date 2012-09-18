package seep.infrastructure;

import java.net.InetAddress;
import java.io.Serializable;

/**
* Node. Node class models a physical node with its IP and port number.
*/

public class Node implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private InetAddress ip;
	private int port;
	
	public InetAddress getIp(){
		return ip;
	}	

	public int getPort() {
		return port;
	}

	public Node setIp(InetAddress newIp){
		return new Node(newIp, port);
	}

	public Node(InetAddress ip, int port) {
		super();
		this.ip = ip;
		this.port = port;
	}
	
	@Override public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
		result = prime * result + port;
		return result;
	}

	@Override public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Node other = (Node) obj;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		if (port != other.port)
			return false;
		return true;
	}

	@Override public String toString() {
		return "Node [ip=" + ip + ", port=" + port + "]";
	}

}
