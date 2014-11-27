package uk.ac.imperial.lsds.seepmaster;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

import joptsimple.OptionParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.comm.serialization.JavaSerializer;
import uk.ac.imperial.lsds.seep.config.CommandLineArgs;
import uk.ac.imperial.lsds.seep.config.ConfigKey;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.comm.MasterWorkerAPIImplementation;
import uk.ac.imperial.lsds.seepmaster.comm.MasterWorkerCommManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManagerFactory;
import uk.ac.imperial.lsds.seepmaster.query.QueryManager;
import uk.ac.imperial.lsds.seepmaster.ui.UI;
import uk.ac.imperial.lsds.seepmaster.ui.UIFactory;


public class Main {
	
	final private static Logger LOG = LoggerFactory.getLogger(Main.class);

	private void executeMaster(String[] args, MasterConfig mc){
		int infType = mc.getInt(MasterConfig.DEPLOYMENT_TARGET_TYPE);
		LOG.info("Deploy target of type: {}", InfrastructureManagerFactory.nameInfrastructureManagerWithType(infType));
		InfrastructureManager inf = InfrastructureManagerFactory.createInfrastructureManager(infType);
		// TODO: get file from config if exists and parse it to get a map from operator to endPoint
		Map<Integer, EndPoint> mapOperatorToEndPoint = null;
		// TODO: from properties get serializer and type of thread pool and resources assigned to it
		Comm comm = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
		QueryManager qm = QueryManager.getInstance(inf, mapOperatorToEndPoint, comm);
		// TODO: put this in the config manager
		int port = mc.getInt(MasterConfig.LISTENING_PORT);
		MasterWorkerAPIImplementation api = new MasterWorkerAPIImplementation(qm, inf);
		MasterWorkerCommManager mwcm = new MasterWorkerCommManager(port, api);
		mwcm.start();
		int uiType = mc.getInt(MasterConfig.UI_TYPE);
		UI ui = UIFactory.createUI(uiType, qm, inf);
		LOG.info("Created UI of type: {}", UIFactory.nameUIOfType(uiType));
		String queryPathFile = mc.getString(MasterConfig.QUERY_FILE);
		String baseClass = mc.getString(MasterConfig.BASECLASS_NAME);
		LOG.info("Loading query {} with baseClass: {} from local file...", queryPathFile, baseClass);
		qm.loadQueryFromFile(queryPathFile, baseClass);
		LOG.info("Loading query...OK");
		ui.start();		
	}
	
	public static void main(String args[]){
		// Get Properties with command line configuration 
		List<ConfigKey> configKeys = MasterConfig.getAllConfigKey();
		OptionParser parser = new OptionParser();
		parser.accepts(MasterConfig.QUERY_FILE, "Jar file with the compiled SEEP query").withRequiredArg();
		parser.accepts(MasterConfig.BASECLASS_NAME, "Name of the Base Class").withRequiredArg();
		CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
		Properties commandLineProperties = cla.getProperties();
		
		// Get Properties with file configuration
		Properties fileProperties = null;
		if(commandLineProperties.containsKey(MasterConfig.PROPERTIES_FILE)){
			String propertiesFile = commandLineProperties.getProperty(MasterConfig.PROPERTIES_FILE);
			fileProperties = Utils.readPropertiesFromFile(propertiesFile, false);
		}
		else{
			fileProperties = Utils.readPropertiesFromFile("config.properties", true);
		}
		
		// Merge both properties, command line has preference
		Properties validatedProperties = Utils.overwriteSecondPropertiesWithFirst(commandLineProperties, fileProperties);
		// TODO: validate properties, making sure all required are there
		MasterConfig mc = new MasterConfig(validatedProperties);
		Main instance = new Main();
		instance.executeMaster(args, mc);
	}
}