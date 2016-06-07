package orca.comet.accumulo.genc;

import java.util.UUID;
import java.io.IOException;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.thrift.ThriftTest.AsyncProcessor.throwsError;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import orca.comet.accumulo.client.ClientConfigProps;
import orca.comet.accumulo.genc.COMETClientConst;
import orca.comet.accumulo.genc.utils.COMETClientUtils;

public class COMETGenCImpl implements COMETGenCIfce {
	private static final Logger log = Logger.getLogger(COMETGenCImpl.class);
	static Properties props;
	String configFile = "/Users/claris/git/COMET/comet-accumulo-genc/src/test/resources/configFile";
	String mainTable = "virtualsystems";
	private Connector cometConnector;

	private final String keystore;
	private final String truststore;
	private final String keystorePass;
	private final String truststorePass;
	private final String accumuloInstance;
	private final String accumuloUser;
	private final String accumuloPass;
	private final String zookeeperHosts;
	private final String interfaceTable;
	private final Long memBuffer;
	private final int numberOfThreads;

	public COMETGenCImpl(String configurationFile) {

		try {
			props = COMETClientUtils.getClientConfigProps(configurationFile);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		keystore = props
				.getProperty(ClientConfigProps.COMET_CLIENT_KEYSTORE_PROP);
		truststore = props
				.getProperty(ClientConfigProps.COMET_CLIENT_TRUSTTORE_PROP);
		keystorePass = props
				.getProperty(ClientConfigProps.COMET_CLIENT_KEYSTORE_PASS_PROP);
		truststorePass = props
				.getProperty(ClientConfigProps.COMET_CLIENT_TRUSTSTORE_PASS_PROP);
		accumuloInstance = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ACCUMULO_INSTANCE_PROP);
		mainTable = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ACCUMULO_MAINTABLE_PROP);
		interfaceTable = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ACCUMULO_IFCETABLE_PROP);
		accumuloUser = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ACCUMULO_USER_DEFAULT);
		accumuloPass = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ACCUMULO_USER_DEFAULT_PASS);
		zookeeperHosts = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ZOOKEEPERS_HOSTS_PROP);
		memBuffer = Long.valueOf(props.getProperty(
				ClientConfigProps.COMET_CLIENT_ACCUMULO_MEMBUFF,
				COMETClientConst.ACCUMULO_CLIENT_BUFF_DEFAULT));

		numberOfThreads = Integer.valueOf(props.getProperty(
				ClientConfigProps.COMET_CLIENT_ACCUMULO_NUMTHREADS,
				COMETClientConst.ACCUMULO_CLIENT_NUMTHREADS_DEFAULT));
		/*
		 * Client configuration with properties passed in via configuration file
		 */
		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.withSsl(true);
		clientConf.withKeystore(keystore, keystorePass, "JKS");
		clientConf.withTruststore(truststore, truststorePass, "JKS");
		clientConf.withInstance(accumuloInstance);
		clientConf.withZkHosts(zookeeperHosts);

		// Accumulo instance creation
		log.setLevel(Level.DEBUG);

		Instance instance = new ZooKeeperInstance(clientConf);

		try {
			// Accumulo connector to instance. User and password are needed in
			// addition to SSL certificates.

			cometConnector = instance.getConnector(accumuloUser, accumuloPass);

			TableOperations tableOps = cometConnector.tableOperations();
			if (!tableOps.exists(mainTable)) {
				log.info("Table " + mainTable
						+ " does not exist. Creating table.");
				tableOps.create(mainTable);
			}
			if (!tableOps.exists(interfaceTable)) {
				log.info("Table " + interfaceTable
						+ " does not exist. Creating table.");
				tableOps.create(interfaceTable);
			}

		} catch (AccumuloException e) {
			System.out
					.println("Client failed to obtain connector to Accumulo instance");
			log.error("Client failed to obtain connector to Accumulo instance "
					+ instance + ".");
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			System.out
					.println("Client failed to authenticate with Accumulo to connect to instance");
			log.error("Client failed to authenticate with Accumulo to connect to instance "
					+ instance + ".");
			e.printStackTrace();
		} catch (TableExistsException e) {
			log.error("Error creating table. Reason: " + e.getMessage());

		}

	}

	public String createScope(String contextType, String contextSubType,
			String contextID, String scopeName, String scopeValue,
			String visibility) {
		BatchWriter bw = null;
		Mutation mut1 = new Mutation(new Text(contextID));
		Text colFam2 = new Text(contextSubType);
		Text ColFam2ColQual1 = new Text(scopeName);
		mut1.put(colFam2, ColFam2ColQual1, new Value(scopeValue.getBytes()));

		try {
			bw = createBatchWriter(contextType);
			bw.addMutation(mut1);
			bw.close(); // flushes and release ---no need for bw.flush()
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in creating new entry (" + contextID
					+ ")");
			e.printStackTrace();
		}

		return contextID;
	}

	/** 
	 * @TODO: If delterow returns a null we have to return an empty string. 
	 * JSON error messages
	 */
	public String destroyScope(String contextType, String contextSubType,
			String contextID, String scopeName, String visibility) {
		Authorizations auth = new Authorizations();
		String key=null;

		try {
			Scanner scan = cometConnector.createScanner(contextType, auth);
			scan.setRange(new Range(contextID, contextID));

			key =  deleteRow(scan, contextType);
			

		} catch (TableNotFoundException e) {
			log.error("DeleteEntry failed due to: " + e.getMessage());
		}
	
		if(key == null) {
			return ("Scope not found.");
		}
		else return key;
	}

	private String deleteRow(Scanner scanner, String table) {
		Mutation deleter = null;
		for (Entry<Key, Value> entry : scanner) {
			if (deleter == null) {
				deleter = new Mutation(entry.getKey().getRow());
			}
			deleter.putDelete(entry.getKey().getColumnFamily(), entry.getKey()
					.getColumnQualifier());

		}
		try {
			if(deleter!=null) {
				BatchWriter bw = createBatchWriter(table);
				bw.addMutation(deleter);
				bw.close();
			}
			
		} catch (MutationsRejectedException e) {
			log.error("Error deleting record. Reason: " + e.getMessage());
		}
		
		if(deleter!=null){
			return deleter.getRow().toString();
		}
		
		return null;
	}

	public String readScope(String contextType, String contextSubType, String contextID,
			String scopeName, String visibility) {
		
		Authorizations auths = new Authorizations();
		Text ColFam = new Text(contextSubType);
		Text ColFamQual = new Text(scopeName);
		String scopeValue = null;
		try {
			Scanner scan = cometConnector.createScanner(contextType, auths);
			scan.setRange(new Range(contextID, contextID));
			scan.fetchColumn(ColFam, ColFamQual);

			for (Entry<Key, Value> entry : scan) {
				scopeValue = entry.getValue().toString();
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + contextType);
			e.printStackTrace();
		}

		return scopeValue;
	}

	public String modifyScope(String contextType, String contextSubType,String contextID,
			String scopeName, String newScopeValue, String visibility) {
		BatchWriter bw = null;
		Mutation mut1 = new Mutation(new Text(contextID));
		Text colFam2 = new Text(contextSubType);
		Text ColFam2ColQual1 = new Text(scopeName);
		mut1.put(colFam2, ColFam2ColQual1, new Value(newScopeValue.getBytes()));

		try {
			bw = createBatchWriter(contextType);
			bw.addMutation(mut1);
			bw.close(); // flushes and release ---no need for bw.flush()
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in updating  entry (" + contextID
					+ ")");
			e.printStackTrace();
		}

		return contextID;
		
	}

	/**
	 * 
	 * @param {string} table
	 * @return BatchWrtier to effect mutations against COMET service on
	 * @param{table
	 */
	private BatchWriter createBatchWriter(String table) {
		BatchWriter bw = null;

		BatchWriterConfig bwConfig = new BatchWriterConfig();
		// bwConfig.setMaxMemory(memBuffer);
		bwConfig.setTimeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		// bwConfig.setMaxWriteThreads(numberOfThreads);
		try {
			bw = cometConnector.createBatchWriter(table, bwConfig);
		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + table
					+ " to create batchWriter.");
			e.printStackTrace();
		}

		return bw;
	}

	/**
	 * Helper function to create rowid by concatenating sliceid and
	 * reservationid
	 * 
	 * @param sliceID
	 * @param reservationID
	 * @return {Text} rowid consisting of the concatenation of sliceID and
	 *         reservationID
	 */
	private static Text createRowId(String sliceID, String reservationID) {
		/**
		 * @TODO: verify sliceID and reservationID format
		 */
		return new Text(sliceID + ":" + reservationID);
	}

	public static void main(String args[]) {
		String sliceid = UUID.randomUUID().toString();
		String scopeid = UUID.randomUUID().toString();
		String scopevalue = UUID.randomUUID().toString();
		COMETGenCImpl cimpl = new COMETGenCImpl(
				"/Users/claris/git/COMET/comet-accumulo-genc/src/test/resources/configFile");
		System.out.println("cratescope");
		cimpl.createScope("virtualsystems", "iaas", sliceid, scopeid,
				scopevalue, null);
		System.out.println("destroyscope");
		cimpl.destroyScope("virtualsystems", "iaas", "0f6fde07-90df-4128-97a3-9b983c552d0f", "60d311eb-afee-4513-9251-5e3cca21fe77", null);
		System.out.println("updateScope");
		cimpl.modifyScope("virtualsystems", "iaas", "e0b088d5-69d3-42f2-b143-5a63574c05b0",
				"03542ec9-9669-43da-ad03-01baa8b26b6a", "newScopeValue++", null);
	
		System.out.println("readScope");
		System.out.println("ScopeValue is: " + cimpl.readScope("virtualsystems", "iaas", 
				"e0b088d5-69d3-42f2-b143-5a63574c05b0",
				"03542ec9-9669-43da-ad03-01baa8b26b6a", null));
	}
	
	
	class NonExistentRecords extends Exception {
		
		private static final long serialVersionUID = -4940370878206181637L;

		public NonExistentRecords(String msg){
		      super(msg);
		   }
		}

}
