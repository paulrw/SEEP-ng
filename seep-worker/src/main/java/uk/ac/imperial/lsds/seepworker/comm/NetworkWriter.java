package uk.ac.imperial.lsds.seepworker.comm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;

public class NetworkWriter {

	private Selector selector;
	
	private boolean working = false;
	
	// this will actually receive whatever objects we want to attach to the key
	public NetworkWriter(List<Connection> cons, Map<Integer, OutputBuffer> outputBuffers){
		try{
			this.selector = Selector.open();
			for(Connection c : cons){
				SocketChannel channel = SocketChannel.open();
				InetSocketAddress address = c.getInetSocketAddress();
				
		        Socket socket = channel.socket();
		        socket.setKeepAlive(true);
		        socket.setTcpNoDelay(true);
		        try {
		            channel.connect(address);
		        } 
		        catch (UnresolvedAddressException uae) {
		            channel.close();
		            throw new IOException("The provided address cannot be resolved: " + address, uae);
		        } 
		        catch (IOException io) {
		            channel.close();
		            throw io;
		        }
				
				channel.configureBlocking(false);
				int interestSet = SelectionKey.OP_CONNECT;
				SelectionKey key = channel.register(selector, interestSet);
				// Receiver r = new Receiver()
				key.attach(this);
			}
		}
		catch(IOException io){
			io.printStackTrace();
		}
	}
	
	class Worker implements Runnable{

		@Override
		public void run() {
			while(working){
				try {
					int readyChannels = selector.select();
					while(readyChannels == 0){ 
						continue;
					}
					Set<SelectionKey> selectedKeys = selector.selectedKeys();
					Iterator<SelectionKey> keyIt = selectedKeys.iterator();
					while(keyIt.hasNext()){
						SelectionKey key = keyIt.next();
						// connect events
						if(key.isConnectable()){
							SocketChannel sc = (SocketChannel) key.channel();
							sc.finishConnect();
							key.interestOps(SelectionKey.OP_WRITE);
						}
						// write events
						if(key.isWritable()){
							
						}
					}
					
					keyIt.remove();
				} 
				catch (IOException e) {
					e.printStackTrace();
				}	
			}
		}
		
	}
	
}
