package uk.ac.imperial.lsds.seepworker.comm;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.PhysicalSeepQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.KryoFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.Command;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.QueryDeployCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StartQueryCommand;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Conductor;

import com.esotericsoftware.kryo.Kryo;

public class WorkerMasterAPIImplementation {

	final private Logger LOG = LoggerFactory.getLogger(WorkerMasterAPIImplementation.class.getName());
	
	private Conductor c;
	private Comm comm;
	private Kryo k;
	
	private String myIp;
	private int myPort;
	private int retriesToMaster;
	private int retryBackOffMs;
	
	public WorkerMasterAPIImplementation(Comm comm, Conductor c, WorkerConfig wc){
		this.comm = comm;
		this.c = c;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
		this.myPort = wc.getInt(WorkerConfig.LISTENING_PORT);
		this.retriesToMaster = wc.getInt(WorkerConfig.MASTER_CONNECTION_RETRIES);
		this.retryBackOffMs = wc.getInt(WorkerConfig.MASTER_RETRY_BACKOFF_MS);
	}
	
	public void bootstrap(Connection masterConn, String myIp, int myPort, int dataPort){
		this.myIp = myIp;
		Command command = ProtocolCommandFactory.buildBootstrapCommand(myIp, myPort, dataPort);
		
		for (int i = 0; i < retriesToMaster; i++) {
			System.out.println("sending bootstrap, attemps: "+i);
			boolean success = comm.send_object_async(command, masterConn, k);
			if(success){
				System.out.println("conn success");
				return;
			}
			try {
				Thread.sleep(retryBackOffMs);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// TODO: throw exception here to indicate failure
	}
	
	public void handleQueryDeploy(QueryDeployCommand qdc){
		InetAddress ip = null;
		try {
			ip = InetAddress.getByName(myIp);
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		int myOwnId = Utils.computeIdFromIpAndPort(ip, myPort);
		PhysicalSeepQuery query = qdc.getQuery();
		
		/**	OLD STUFF WE USED TO DO:
		 * things to do:
		 * - build operator out of physicalOperator
		 * - configure router statically
		 * - make data ingestion local to op
		 * - same for sink
		 * - create star topology and push it
		 * - instantiation of one operator
		 * - initialization of one operator
		 */
		
		// We don't know yet what is this for anyway...
		Set<EndPoint> meshTopology = query.getMeshTopology(myOwnId);
		
		PhysicalOperator po = query.getOperatorLivingInExecutionUnitId(myOwnId);
		LOG.info("Found PhysicalOperator: {} to execute in this executionUnit: {}", po.getOperatorName(), myOwnId);
		c.deployPhysicalOperator(po, query);
	}

	public void handleStartQuery(StartQueryCommand sqc) {
		c.startProcessing();
	}
	
}
