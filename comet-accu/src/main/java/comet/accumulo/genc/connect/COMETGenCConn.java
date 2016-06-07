package comet.accumulo.genc.connect;

/**
 * 
 */
import java.io.IOException;
import java.util.Properties;

//import orca.comet.accumulo.client.COMETClientConst;
//import orca.comet.accumulo.client.COMETClientImpl;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import comet.accumulo.genc.COMETClientConst;
import comet.accumulo.genc.ClientConfigProps;
import comet.accumulo.genc.utils.COMETClientUtils;

public class COMETGenCConn {

	private final boolean withSSL = true;

	private final String keystore;
	private final String keystoreType;
	private final String truststore;
	private final String truststoreType;
	private final String keystorePass;
	private final String truststorePass;
	private final String accumuloInstance;
	private final String accumuloUser;
	private final String accumuloPass;
	private final String zookeeperHosts;

	private Connector cometConnector;
	private final Long memBuffer;
	private final int numberOfThreads;

	private static final Logger log = Logger.getLogger(COMETGenCConn.class);
	Properties props = null;

	public COMETGenCConn(String configurationFile) {

		try {
			props = COMETClientUtils.getClientConfigProps(configurationFile);
		} catch (IOException e) {
			log.error("Problem reading configuration property file.");
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
		accumuloUser = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ACCUMULO_USER_DEFAULT);
		accumuloPass = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ACCUMULO_USER_DEFAULT_PASS);
		zookeeperHosts = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ZOOKEEPERS_HOSTS_PROP);
		keystoreType = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ACCUMULO_KEYSTORE_TYPE);
		truststoreType = props
				.getProperty(ClientConfigProps.COMET_CLIENT_ACCUMULO_TRUSTSTORE_TYPE);
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
		clientConf.withSsl(withSSL);
		clientConf.withKeystore(keystore, keystorePass, keystoreType);
		clientConf.withTruststore(truststore, truststorePass, keystoreType);
		clientConf.withInstance(accumuloInstance);
		clientConf.withZkHosts(zookeeperHosts);
		

		// Accumulo instance creation
		log.setLevel(Level.DEBUG);

		Instance instance = new ZooKeeperInstance(clientConf);

		try {
			// Accumulo connector to instance. User and password are needed in
			// addition to SSL certificates.
			cometConnector = instance.getConnector(accumuloUser, accumuloPass);
			

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
		}

	}
	
	public static void main(String[] args) {
		COMETGenCConn myConnector = 
				new COMETGenCConn("/Users/claris/git/COMET/comet-accumulo-genc/src/test/resources/configFile");
		
	}
}
