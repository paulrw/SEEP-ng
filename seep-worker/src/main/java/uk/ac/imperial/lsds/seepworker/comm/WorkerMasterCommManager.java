package uk.ac.imperial.lsds.seepworker.comm;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.KryoFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.CodeCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.Command;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolAPI;
import uk.ac.imperial.lsds.seep.comm.protocol.QueryDeployCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StartQueryCommand;
import uk.ac.imperial.lsds.seep.infrastructure.RuntimeClassLoader;
import uk.ac.imperial.lsds.seep.util.Utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class WorkerMasterCommManager {

	final private Logger LOG = LoggerFactory.getLogger(WorkerMasterCommManager.class.getName());
	
	private ServerSocket serverSocket;
	private Kryo k;
	private Thread listener;
	private boolean working = false;
	private WorkerMasterAPIImplementation api;
	private RuntimeClassLoader rcl;
	
	public WorkerMasterCommManager(int port, WorkerMasterAPIImplementation api, RuntimeClassLoader rcl){
		this.api = api;
		this.rcl = rcl;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol(rcl);
		try {
			serverSocket = new ServerSocket(port);
			LOG.info(" Listening on {}:{}", InetAddress.getLocalHost(), port);
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		listener = new Thread(new CommMasterWorker());
		listener.setName(CommMasterWorker.class.getSimpleName());
	}
	
	public void start(){
		this.working = true;
		this.listener.start();
	}
	
	public void stop(){
		//TODO: do some other cleaning work here
		this.working = false;
	}
	
	class CommMasterWorker implements Runnable{

		@Override
		public void run() {
			while(working){
				Socket incomingSocket = null;
				PrintWriter out = null;
				try{
					// Blocking call
					incomingSocket = serverSocket.accept();
					InputStream is = incomingSocket.getInputStream();
					out = new PrintWriter(incomingSocket.getOutputStream(), true);
					Input i = new Input(is, 1000000);
					Command c = k.readObject(i, Command.class);
					short cType = c.type();
					LOG.debug("RX command with type: {}", cType);
					if(cType == ProtocolAPI.CODE.type()){
						LOG.info("RX Code command");
						CodeCommand cc = c.getCodeCommand();
						byte[] file = cc.getData();
						LOG.info("Received query file with size: {}", file.length);
						if(cc.getDataSize() != file.length){
							// sanity check
							// TODO: throw error
						}
						// TODO: get filename from properties file
						File f = Utils.writeDataToFile(file, "query.jar");
						out.println("ack");
						loadCodeToRuntime(f);
					}
					else if(cType == ProtocolAPI.QUERYDEPLOY.type()){
						LOG.info("RX QueryDeploy command");
						QueryDeployCommand qdc = c.getQueryDeployCommand();
						
//						//test
//						PhysicalSeepQuery psq = qdc.getQuery();
//						for(PhysicalOperator po : psq.getOperators()){
//							DataTuple a = null;
//							po.getSeepTask().processData(a);
//						}
//						System.out.println("QUERY: "+psq.toString());
						out.println("ack");
						api.handleQueryDeploy(qdc);
					}
					else if(cType == ProtocolAPI.STARTQUERY.type()){
						LOG.info("RX StartRuntime command");
						StartQueryCommand sqc = c.getStartQueryCommand();
						out.println("ack");
						api.handleStartQuery(sqc);
					}
				}
				catch(IOException io){
					io.printStackTrace();
				}
				finally {
					if (incomingSocket != null){
						try {
							incomingSocket.close();
						}
						catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}		
		}	
	}	
	
	private void loadCodeToRuntime(File pathToCode){
		URL urlToCode = null;
		try {
			urlToCode = pathToCode.toURI().toURL();
			System.out.println("Loading into class loader: "+urlToCode.toString());
			URL[] urls = new URL[1];
			urls[0] = urlToCode;
			rcl.addURL(urlToCode);
		}
		catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}
}
