package orca.comet.accumulo.genc.test;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import orca.comet.accumulo.client.ClientConfigProps;
import orca.comet.accumulo.genc.COMETClientConst;
import orca.comet.accumulo.genc.COMETGenCImpl;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class basicTest {
	private static final Logger log = Logger.getLogger(basicTest.class);
	private static COMETGenCImpl genc;
	private static String mainTable = "virtualsystems";
	private static Connector cometConnector;

	private static Properties props;
	private static String keystore;
	private static String truststore;
	private static String keystorePass;
	private static String truststorePass;
	private static String accumuloInstance;
	private static String accumuloUser;
	private static String accumuloPass;
	private static String zookeeperHosts;
	private static String interfaceTable;
	private static Long memBuffer;
	private static int numberOfThreads;

	@BeforeClass
	public static void initClient() {
		genc = new COMETGenCImpl(null);

		/**
		 * We should create an outbound connection to accumulo. This would also
		 * allow us to check the database
		 */

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
		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.withSsl(true);
		clientConf.withKeystore(keystore, keystorePass, "JKS");
		clientConf.withTruststore(truststore, truststorePass, "JKS");
		clientConf.withInstance(accumuloInstance);
		clientConf.withZkHosts(zookeeperHosts);

		Instance instance = new ZooKeeperInstance(clientConf);

		try {
			// Accumulo connector to instance. User and password are needed in
			// addition to SSL certificates.

			cometConnector = instance.getConnector(accumuloUser, accumuloPass);

			TableOperations tableOps = cometConnector.tableOperations();
			if (!tableOps.exists(mainTable)) {
				tableOps.create(mainTable);
			}
		} catch (AccumuloException e) {

			log.error("Client failed to obtain connector to Accumulo instance "
					+ instance + ".");
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {

			log.error("Client failed to authenticate with Accumulo to connect to instance "
					+ instance + ".");
			e.printStackTrace();
		} catch (TableExistsException e) {
			log.error("Error creating table. Reason: " + e.getMessage());

		}
		
		String contextID = "contextID";
		String contextSubType = "contextSubType";
		String scopeName = "scopeName";
		String scopeValue = "scopeValue";
		String contextType = "contextType";
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


	}
	private static BatchWriter createBatchWriter(String table) {
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
	
	@After
	public void afterEachTest() {
		// Remove contextID if it exists.
	}

	@Test
	public void testCreateScope() {
		genc.createScope("contextType", "contextSubType", "contextID",
				"scopeName", "scopeValue", null);
		String value = genc.readScope("contextType", "contextSubType",
				"contextID", "scopeName", null);
		assertEquals("scapeValue", value);
	}

	@Test
	public void testReadScope() {
		genc.createScope("contextType", "contextSubType", "contextID",
				"scopeName", "scopeValue", null);
		String value = genc.readScope("contextType", "contextSubType",
				"contextID", "scopeName", null);
		assertEquals("scapeValue", value);
	}

	@Test
	public void testModifyScope() {
		genc.createScope("contextType", "contextSubType", "contextID",
				"scopeName", "scopeValue", null);
		genc.modifyScope("contextType", "contextSubType", "contextID",
				"scopeName", "newScopeValue", null);
		String value = genc.readScope("contextType", "contextSubType",
				"contextID", "scopeName", null);
		assertEquals("newScopeValue", value);
	}

	@Test
	public void testDestroyScope() {
		genc.createScope("contextType", "contextSubType", "contextID",
				"scopeName", "scopeValue", null);
		String contextID = genc.destroyScope("contextType", "contextSubType",
				"contextID", "scopeName", null);
		assertEquals("contextID", contextID);
	}
}
