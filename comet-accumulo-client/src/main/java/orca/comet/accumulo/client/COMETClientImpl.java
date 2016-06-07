package orca.comet.accumulo.client;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import orca.comet.accumulo.client.utils.COMETClientUtils;

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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * 
 * @author claris
 * @TODO: Enable with sasl support (no ssl) as an alternative for when there are
 *        no cert available.
 * @TODO: Overwrite mode for update operations. Is it needed?
 * @TODO: logging
 */
public class COMETClientImpl implements COMETClientIfce {

	private final String mainTable;
	private final String keystore;
	private final String truststore;
	private final String keystorePass;
	private final String truststorePass;
	private final String accumuloInstance;
	private final String accumuloUser;
	private final String accumuloPass;
	private final String zookeeperHosts;
	private final String interfaceTable;
	private Connector cometConnector;
	private final Long memBuffer;
	private final int numberOfThreads;

	private static final Logger log = Logger.getLogger(COMETClientImpl.class);

	@SuppressWarnings("deprecation")
	public COMETClientImpl(Properties props) {

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
		
			if(!tableOps.exists(mainTable)) {
				System.out.println("Table mainTable does not exist!");
				log.info("Table "+ mainTable + " does not exist. Creating table.");
				tableOps.create(mainTable);
				System.out.println("Table created!");
			} 
			System.out.println("Table mainTable interfaces exists!");
			if (!tableOps.exists(interfaceTable)) {
				System.out.println("Table interfaces does not exist!");
				log.info("Table "+ interfaceTable + " does not exist. Creating table.");
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

	
	/**
	 * @see {@link COMETClientIfce#setHostname(String, String, String)}
	 */
	public void setHostname(String SliceID, String ReservationID,
			String hostname) {
		Text rowid = createRowId(SliceID, ReservationID);
		Mutation mut1 = new Mutation(rowid);

		Text colFam = new Text(COMETClientConst.COLFAM_USER);
		Text colQual = new Text(COMETClientConst.MAIN_COLQUAL_HOSTNAME);
		Value value = new Value(hostname.getBytes());
		mut1.put(colFam, colQual, value);

		try {
			BatchWriter bw = createBatchWriter(mainTable);
			bw.addMutation(mut1);
			bw.close();
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in setting hostname (" + hostname
					+ ") for resource " + rowid.toString());
			e.printStackTrace();

		}

	}

	/**
	 * @see{@link{@link #updateHostname(String, String, String, String)}}
	 */
	public void updateHostname(String SliceID, String ReservationID,
			String hostname, String overwrite) {
		setHostname(SliceID, ReservationID, hostname);

	}

	/**
	 * @see{@link #setScript(String, String, String)}
	 */
	public void setScript(String SliceID, String ReservationID, String script) {
		Text rowid = createRowId(SliceID, ReservationID);
		Mutation mut1 = new Mutation(rowid);

		Text colFam = new Text(COMETClientConst.COLFAM_USER);
		Text colQual = new Text(COMETClientConst.MAIN_COLQUAL_SCRIPT);
		Value value = new Value(script.getBytes());
		mut1.put(colFam, colQual, value);

		try {
			BatchWriter bw = createBatchWriter(mainTable);
			bw.addMutation(mut1);
			bw.close();
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in setting script (" + script
					+ ") for resource " + rowid.toString());

			e.printStackTrace();

		}

	}

	/**
	 * @see {@link #setUsers(String, String, String)}
	 */
	public void setUsers(String SliceID, String ReservationID, String users) {
		// TODO Auto-generated method stub

	}

	/**
	 * @see {@link #updateUsers(String, String, String, String)}
	 */
	public void updateUsers(String SliceID, String ReservationID, String users,
			String overwrite) {
		// TODO Auto-generated method stub

	}

	/**
	 * @see {@link #setManagementIP(String, String, String)}
	 */
	public void setManagementIP(String SliceID, String ReservationID, String ip) {
		Text rowid = createRowId(SliceID, ReservationID);
		Mutation mut1 = new Mutation(rowid);

		Text colFam = new Text(COMETClientConst.COLFAM_SYSTEM);
		Text colQual = new Text(COMETClientConst.MAIN_COLQUAL_MGMTIP);
		Value value = new Value(ip.getBytes());
		mut1.put(colFam, colQual, value);

		try {
			BatchWriter bw = createBatchWriter(mainTable);
			bw.addMutation(mut1);
			bw.close();
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in setting management ip (" + ip
					+ ") for resource " + rowid.toString());

			e.printStackTrace();

		}

	}

	/**
	 * @see {@link COMETClientIfce#updateManagementIP(String, String, String, String)}
	 */
	public void updateManagementIP(String SliceID, String ReservationID,
			String ip, String overwrite) {
		setManagementIP(SliceID, ReservationID, ip);
	}

	/**
	 * @see {@link COMETClientIfce}
	 *      {@link #setPhysicalHost(String, String, String)}
	 */
	public void setPhysicalHost(String SliceID, String ReservationID,
			String physicalHost) {

		Text rowid = createRowId(SliceID, ReservationID);
		Mutation mut1 = new Mutation(rowid);

		Text colFam = new Text(COMETClientConst.COLFAM_SYSTEM);
		Text colQual = new Text(COMETClientConst.MAIN_COLQUAL_PHYSICAL_HOST);
		Value value = new Value(physicalHost.getBytes());
		mut1.put(colFam, colQual, value);

		try {
			BatchWriter bw = createBatchWriter(mainTable);
			bw.addMutation(mut1);
			bw.close();
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in setting physical host ("
					+ physicalHost + ") for resource " + rowid.toString());

			e.printStackTrace();

		}

	}

	/**
	 * @see @link {@link COMETClientIfce}
	 *      {@link #updatePhysicalHost(String, String, String)}
	 */

	public void updatePhysicalHost(String SliceID, String ReservationID,
			String physicalHost) {
		setPhysicalHost(SliceID, ReservationID, physicalHost);

	}

	/**
	 * @see @link{ COMETClientIfce#setNovaID(String, String, String)}
	 */
	public void setNovaID(String SliceID, String ReservationID, String novaid) {
		Text rowid = createRowId(SliceID, ReservationID);
		Mutation mut1 = new Mutation(rowid);

		Text colFam = new Text(COMETClientConst.COLFAM_SYSTEM);
		Text colQual = new Text(COMETClientConst.MAIN_COLQUAL_NOVA_ID);
		Value value = new Value(novaid.getBytes());
		mut1.put(colFam, colQual, value);

		try {
			BatchWriter bw = createBatchWriter(mainTable);
			bw.addMutation(mut1);
			bw.close();
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in setting nova id (" + novaid
					+ ") for resource " + rowid.toString());

			e.printStackTrace();
		}

	}

	/**
	 * @see {@link COMETClientIfce#updateNovaID(String, String, String)}
	 */
	public void updateNovaID(String SliceID, String ReservationID, String novaid) {
		setNovaID(SliceID, ReservationID, novaid);
	}

	/**
	 * @see {@link COMETClientIfce#createNewEntry(String, String, String)}
	 */
	public void createNewEntry(String SliceID, String ReservationID, String type) {
		BatchWriter bw = null;

		Text rowid = createRowId(SliceID, ReservationID);
		Mutation mut1 = new Mutation(rowid);

		Text colFam2 = new Text(COMETClientConst.COLFAM_SYSTEM);
		Text ColFam2ColQual1 = new Text(
				COMETClientConst.COLFAM_SYSTEM_COLQUAL_IFCES);
		Text ColFam2ColQual2 = new Text(
				COMETClientConst.MAIN_COLQUAL_RSRCE_TYPE);
		Text ColFam2ColQual3 = new Text(
				COMETClientConst.COLFAM_SYSTEM_COLQUAL_IFCES);

		mut1.put(colFam2, ColFam2ColQual1, new Value(Integer.toString(0)
				.getBytes()));
		mut1.put(colFam2, ColFam2ColQual2, new Value(type.getBytes()));
		mut1.put(colFam2, ColFam2ColQual3, new Value(Integer.toString(0)
				.getBytes()));

		try {
			bw = createBatchWriter(mainTable);
			bw.addMutation(mut1);

			bw.close(); // flushes and release ---no need for bw.flush()
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in creating new entry (" + rowid + ")");
			e.printStackTrace();
		}

	}

	/**
	 * @see {@link COMETClientIfce#setInterface(String, String, String, String, String, String)}
	 */
	public void setInterface(String SliceID, String ReservationID,
			String interfaceID, String protocol, String ipAddress, String state) {

		int currentCount = 0;

		try {
			Scanner scan = cometConnector.createScanner(mainTable,
					new Authorizations());
			scan.fetchColumn(new Text(COMETClientConst.COLFAM_SYSTEM),
					new Text(COMETClientConst.COLFAM_SYSTEM_COLQUAL_IFCES));

			for (Entry<Key, Value> entry : scan) {
				Value value = entry.getValue();
				currentCount = Integer.valueOf(value.toString());
				currentCount++;
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + mainTable
					+ "when scanning for intefaces.");
			e.printStackTrace();
		}

		Mutation mut1 = new Mutation(createRowId(SliceID, ReservationID));
		Text colFam = new Text(COMETClientConst.COLFAM_SYSTEM);
		Text colFamColQual0 = new Text(
				COMETClientConst.COLFAM_SYSTEM_COLQUAL_IFCES);
		String value0 = String.valueOf(currentCount);
		mut1.put(colFam, colFamColQual0, new Value(value0.getBytes()));// new
																		// count
																		// value

		Text colFamColQual1 = new Text(COMETClientConst.IFCE_PREFIX + value0);
		mut1.put(colFam, colFamColQual1, new Value(interfaceID.getBytes()));
		log.info("Adding interface:ColumnFamily " + colFam
				+ "\t ColumnFamilyQualify" + colFamColQual1 + "\t Value"
				+ interfaceID.getBytes());
		// Lets update the interface Table
		Text rowid = new Text(createRowId(SliceID, ReservationID));
		Mutation mut2 = new Mutation(rowid + ":" + interfaceID);
		Text colFam1 = new Text(COMETClientConst.INTERFACES_COLFAM);
		Text ColFam1ColQual1 = new Text(
				COMETClientConst.INTERFACES_COLQUAL_PROT);
		Text ColFam1ColQual2 = new Text(
				COMETClientConst.INTERFACES_COLQUAL_STATE);
		Text ColFam1ColQual3 = new Text(COMETClientConst.INTERFACES_COLQUAL_IP);
		String value1 = protocol;
		String value2 = state;
		String value3 = ipAddress;
		mut2.put(colFam1, ColFam1ColQual1, new Value(value1.getBytes()));
		mut2.put(colFam1, ColFam1ColQual2, new Value(value2.getBytes()));
		mut2.put(colFam1, ColFam1ColQual3, new Value(value3.getBytes()));

		try {
			BatchWriter bw = createBatchWriter(mainTable);
			bw.addMutation(mut1);
			bw.close();

		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in adding  interface (" + interfaceID
					+ ") for resource " + rowid.toString());

		}
		try {
			BatchWriter bw2 = createBatchWriter(interfaceTable);
			bw2.addMutation(mut2);
			bw2.close();
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in adding  interface data ("
					+ interfaceID + ") for resource " + rowid.toString()
					+ " in table " + interfaceTable);

		}

	}

	/**
	 * @see {@link COMETClientIfce#updateInterfaceIpAddress(String, String, String, String)}
	 */
	public void updateInterfaceIpAddress(String SliceID, String ReservationID,
			String interfaceID, String ipAddress) {
		BatchWriter bw = null;
		Text rowid = createRowId(SliceID, ReservationID);
		try {
			Mutation mut1 = new Mutation(rowid + ":" + interfaceID);

			Text colFam = new Text(COMETClientConst.INTERFACES_COLFAM);
			Text colFamColQual = new Text(
					COMETClientConst.INTERFACES_COLQUAL_IP);
			String value = ipAddress;
			mut1.put(colFam, colFamColQual, new Value(value.getBytes()));
			bw = createBatchWriter(interfaceTable);
			bw.addMutation(mut1);
			bw.close();
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in updating ipaddress of interface ("
					+ interfaceID + ") for resource " + rowid.toString());

		}
	}

	/**
	 * @see {@link COMETClientIfce#updateInterfaceState(String, String, String, String)}
	 */
	public void updateInterfaceState(String SliceID, String ReservationID,
			String interfaceID, String state) {
		BatchWriter bw = null;
		Text rowid = createRowId(SliceID, ReservationID);

		try {
			Mutation mut1 = new Mutation(rowid + ":" + interfaceID);

			Text colFam = new Text(COMETClientConst.INTERFACES_COLFAM);
			Text colFamColQual = new Text(
					COMETClientConst.INTERFACES_COLQUAL_STATE);
			String value = state;
			mut1.put(colFam, colFamColQual, new Value(value.getBytes()));
			bw = createBatchWriter(interfaceTable);
			bw.addMutation(mut1);
			bw.close();
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in updating state of  interface ("
					+ interfaceID + ") for resource " + rowid.toString());

		}
	}

	/**
	 * @see {@link COMETClientIfce#updateInterfaceProtocol(String, String, String, String)}
	 */

	public void updateInterfaceProtocol(String SliceID, String ReservationID,
			String interfaceID, String protocol) {
		BatchWriter bw = null;
		Text rowid = createRowId(SliceID, ReservationID);
		try {
			Mutation mut1 = new Mutation(rowid + ":" + interfaceID);

			Text colFam = new Text(COMETClientConst.INTERFACES_COLFAM);
			Text colFamColQual = new Text(
					COMETClientConst.INTERFACES_COLQUAL_PROT);
			String value = protocol;
			mut1.put(colFam, colFamColQual, new Value(value.getBytes()));
			bw = createBatchWriter(interfaceTable);
			bw.addMutation(mut1);
			bw.close();
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in updating protocol of  interface ("
					+ interfaceID + ") for resource " + rowid.toString());

		}
	}

	/**
	 * Caller method *MUST* close the BatchWrtier. No need to call bw.flush()
	 * when calling bw.close()
	 * 
	 * @return
	 */

	/**
	 * 
	 * @param {string} table
	 * @return BatchWrtier to effect mutations against COMET service on
	 * @param{table
	 */
	private BatchWriter createBatchWriter(String table) {
		BatchWriter bw = null;

		BatchWriterConfig bwConfig = new BatchWriterConfig();
		bwConfig.setMaxMemory(memBuffer);
		bwConfig.setTimeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		bwConfig.setMaxWriteThreads(numberOfThreads);
		try {
			bw = cometConnector.createBatchWriter(table, bwConfig);
		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + table
					+ " to create batchWriter.");
			e.printStackTrace();
		}

		return bw;
	}

	@SuppressWarnings("unused")
	private Text createRowId(String[] fields) {

		Text t = new Text();
		t.append(fields[0].getBytes(), 0, fields[0].length());
		if (fields.length > 1) {
			for (int i = 1; i < fields.length; i++) {
				t.append(":".getBytes(), 0, ":".length());
				t.append(fields[i].getBytes(), 0, fields[i].length());
			}
		}

		return t;
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

	public static void main(String[] args) {

		Properties props = null;
		if (args.length != 1) {
			System.out
					.println("Mising client configuration property file path.");
			System.exit(1);
		}

		try {
			props = COMETClientUtils.getClientConfigProps(args[0]);
		} catch (IOException e) {
			e.printStackTrace();
		}

		COMETClientImpl cometClient = new COMETClientImpl(props);
		
		System.exit(1);;
		//cometClient.deleteEntry("ABC", "CDE");
		//cometClient.deleteEntry("ABC","DEF");
		//cometClient.setInterface("ABC", "CDE", "aa:bb:cc:dd", "ip6", "123.123.123.123", "up");
		cometClient.scanData();
		
		// cometClient.scanData();

		cometClient.deleteEntry("fba32a63-11c2-4967-a495-03aef1bebfed",
				"7136a2a3-d454-4505-adb5-261ebfc6d468");
		cometClient.scanTableMain();
		System.exit(1);
		cometClient.scanTableMain();
		cometClient.scanInterfaces();
		System.exit(1);
		// System.out.println("HOSTNAME : "
		// +cometClient.getHostname("e67d6f9c-16b4-4b6b-b781-5cfd1130dd86","0bab3ccc-a268-4f34-90b0-576026aa89f8"));

		System.out.println("Client created");

		// String slice = UUID.randomUUID().toString();
		// String resv = UUID.randomUUID().toString();
		// System.out.println("new sliceID and reservaitonID pair " + slice
		// +"\t" + resv);
		// cometClient.setInterface("a", "b", "MAC", "Ipv4", "123.123.345.356",
		// "down");

		cometClient.createNewEntry("a", "b", "vm");
		cometClient.setInterface("a", "b", "A-B-C-E", "ipv6",
				"123.123.123.123/20", "up");
		cometClient.setInterface("a", "b", "C-D-E-F", "ipv4", "121.121.2.2/12",
				"down");
		cometClient.setHostname("a", "b", "myvmhostname");
		System.out.println("Hostname: " + cometClient.getHostname("a", "b"));
		cometClient.setManagementIP("a", "b", "1.1.1.1");
		System.out.println("MgmpIP: "
				+ cometClient.getManagementIpAddress("a", "b"));
		cometClient.setNovaID("a", "b", UUID.randomUUID().toString());
		System.out.println("Novaid " + cometClient.getNovaID("a", "b"));
		cometClient.setPhysicalHost("a", "b", "myphysicalhost");
		System.out.println("PM: " + cometClient.getPhysicalHost("a", "b"));

		cometClient.setType("a", "b", "baremetal");
		System.out.println("Type " + cometClient.getType("a", "b"));

		System.out
				.println("INTERFACES  " + cometClient.getInterfaces("a", "b"));

	}

	/**
	 * @see {@link COMETClientIfce#setType(String, String, String)}
	 */
	public void setType(String SliceID, String ReservationID, String type) {
		Text rowid = createRowId(SliceID, ReservationID);
		Mutation mut = new Mutation(createRowId(SliceID, ReservationID));
		Text colFam2 = new Text(COMETClientConst.COLFAM_SYSTEM);
		Text ColFam2ColQual2 = new Text(
				COMETClientConst.MAIN_COLQUAL_RSRCE_TYPE);
		Value value = new Value(type.getBytes());
		mut.put(colFam2, ColFam2ColQual2, value);

		try {
			BatchWriter bw = createBatchWriter(mainTable);
			bw.addMutation(mut);
			bw.close();
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in setting  type (" + type
					+ ") for resource " + rowid.toString());

			e.printStackTrace();
		}

	}

	public String scanInterfaces() {
		Authorizations auths = new Authorizations();
		try {
			Scanner scan2 = cometConnector.createScanner(interfaceTable, auths);
			for (Entry<Key, Value> entry : scan2) {
				System.out.println(entry.getKey().getRow() + " "
						+ entry.getKey().getColumnFamily() + " "
						+ entry.getKey().getColumnQualifier() + "  "
						+ entry.getValue());
			}
			scan2.close();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return accumuloInstance;

	}

	public String scanTableMain() {
		Authorizations auths = new Authorizations();
		try {
			Scanner scan2 = cometConnector.createScanner(mainTable, auths);
			for (Entry<Key, Value> entry : scan2) {
				System.out.print(entry.getKey().getRow() + " ");
				System.out.println(entry.getKey().getColumnFamily() + " "
						+ entry.getKey().getColumnQualifier() + "  "
						+ entry.getValue());
			}
			scan2.close();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return accumuloInstance;

	}

	/**
	 * @see {@link COMETClientIfce#getInterfaces(String, String)}
	 */
	public String getInterfaces(String SliceID, String ReservationID) {
		Authorizations auths = new Authorizations();
		Text rowID = createRowId(SliceID, ReservationID);
		Text t = new Text();

		try {

			Scanner scan2 = cometConnector.createScanner(interfaceTable, auths);
			Text equal = new Text("=");
			for (Entry<Key, Value> entry : scan2) {
				Text rowid = entry.getKey().getRow();
				t.append("{".getBytes(), 0, "{".length());
				if (rowid.toString().contains(ReservationID)) {

					Text colQualifier = entry.getKey().getColumnQualifier();
					Text value = new Text(entry.getValue().toString());

					Text myindex = entry.getKey().getRow();
					t.append(myindex.getBytes(), 0, myindex.getLength());
					t.append(":".getBytes(), 0, ":".length());
					t.append(colQualifier.getBytes(), 0,
							colQualifier.getLength());
					t.append(equal.getBytes(), 0, equal.getLength());
					t.append(value.getBytes(), 0, value.getLength());
				}
				t.append("}".getBytes(), 0, "}".length());
			}
		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + interfaceTable
					+ "when scanning for intefaces of " + rowID.toString());

			e.printStackTrace();
		}

		return t.toString();
	}

	/**
	 * @see {@link COMETClientIfce#updateType(String, String, String)}
	 */
	public void updateType(String SliceID, String ReservationID, String type) {
		setType(SliceID, ReservationID, type);

	}

	@Override
	public String getInterfaceIpAddress(String SliceID, String ReservationID,
			String interfaceID) {
		Authorizations auths = new Authorizations();
		Text rowid = new Text(createRowId(SliceID, ReservationID) + ":"
				+ interfaceID);

		Text ColFam1ColQual = new Text(COMETClientConst.INTERFACES_COLQUAL_IP);
		String ipAddress = null;

		try {
			Scanner scan = cometConnector.createScanner(interfaceTable, auths);
			scan.setRange(new Range(rowid, rowid));

			for (Entry<Key, Value> entry : scan) {
				Text row = entry.getKey().getColumnQualifier();
				if (row.compareTo(ColFam1ColQual.getBytes(), 0,
						ColFam1ColQual.getLength()) == 0) {
					ipAddress = entry.getValue().toString();
				}
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + interfaceTable);
			e.printStackTrace();
		}

		return ipAddress;
	}

	@Override
	public String getInterfaceState(String SliceID, String ReservationID,
			String interfaceID) {
		Authorizations auths = new Authorizations();
		Text rowid = new Text(createRowId(SliceID, ReservationID) + ":"
				+ interfaceID);

		Text ColFam1ColQual = new Text(
				COMETClientConst.INTERFACES_COLQUAL_STATE);
		String state = null;

		try {
			Scanner scan = cometConnector.createScanner(interfaceTable, auths);
			scan.setRange(new Range(rowid, rowid));

			for (Entry<Key, Value> entry : scan) {
				Text row = entry.getKey().getColumnQualifier();
				if (row.compareTo(ColFam1ColQual.getBytes(), 0,
						ColFam1ColQual.getLength()) == 0) {
					state = entry.getValue().toString();
				}
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + interfaceTable);
			e.printStackTrace();
		}

		return state;
	}

	@Override
	public String getInterfaceProtocol(String SliceID, String ReservationID,
			String interfaceID) {
		Authorizations auths = new Authorizations();
		Text rowid = new Text(createRowId(SliceID, ReservationID) + ":"
				+ interfaceID);

		Text ColFam1ColQual = new Text(COMETClientConst.INTERFACES_COLQUAL_PROT);
		String protocol = null;

		try {
			Scanner scan = cometConnector.createScanner(interfaceTable, auths);
			scan.setRange(new Range(rowid, rowid));

			for (Entry<Key, Value> entry : scan) {
				Text row = entry.getKey().getColumnQualifier();
				if (row.compareTo(ColFam1ColQual.getBytes(), 0,
						ColFam1ColQual.getLength()) == 0) {
					protocol = entry.getValue().toString();
				}
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + interfaceTable);
			e.printStackTrace();
		}

		return protocol;
	}

	@Override
	public String getHostname(String SliceID, String ReservationID) {
		Authorizations auths = new Authorizations();
		Text rowid = new Text(createRowId(SliceID, ReservationID));
		Text ColFam = new Text(COMETClientConst.COLFAM_USER);
		Text ColFamQual = new Text(COMETClientConst.MAIN_COLQUAL_HOSTNAME);
		String hostname = null;
		try {
			Scanner scan = cometConnector.createScanner(mainTable, auths);
			scan.setRange(new Range(rowid, rowid));
			scan.fetchColumn(ColFam, ColFamQual);

			for (Entry<Key, Value> entry : scan) {
				hostname = entry.getValue().toString();
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + mainTable);
			e.printStackTrace();
		}

		return hostname;
	}

	@Override
	public String getManagementIpAddress(String SliceID, String ReservationID) {
		Authorizations auths = new Authorizations();
		Text rowid = new Text(createRowId(SliceID, ReservationID));
		Text ColFam = new Text(COMETClientConst.COLFAM_SYSTEM);
		Text ColFamQual = new Text(COMETClientConst.MAIN_COLQUAL_MGMTIP);
		String ipAddress = null;
		try {
			Scanner scan = cometConnector.createScanner(mainTable, auths);
			scan.setRange(new Range(rowid, rowid));
			scan.fetchColumn(ColFam, ColFamQual);

			for (Entry<Key, Value> entry : scan) {
				ipAddress = entry.getValue().toString();
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + mainTable);
			e.printStackTrace();
		}

		return ipAddress;
	}

	@Override
	public String getNovaID(String SliceID, String ReservationID) {
		Authorizations auths = new Authorizations();
		Text rowid = new Text(createRowId(SliceID, ReservationID));
		Text ColFam = new Text(COMETClientConst.COLFAM_SYSTEM);
		Text ColFamQual = new Text(COMETClientConst.MAIN_COLQUAL_NOVA_ID);
		String novaID = null;
		try {
			Scanner scan = cometConnector.createScanner(mainTable, auths);
			scan.setRange(new Range(rowid, rowid));
			scan.fetchColumn(ColFam, ColFamQual);

			for (Entry<Key, Value> entry : scan) {
				novaID = entry.getValue().toString();
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + mainTable);
			e.printStackTrace();
		}

		return novaID;
	}

	@Override
	public String getPhysicalHost(String SliceID, String ReservationID) {
		Authorizations auths = new Authorizations();
		Text rowid = new Text(createRowId(SliceID, ReservationID));
		Text ColFam = new Text(COMETClientConst.COLFAM_SYSTEM);
		Text ColFamQual = new Text(COMETClientConst.MAIN_COLQUAL_PHYSICAL_HOST);
		String physicalHost = null;
		try {
			Scanner scan = cometConnector.createScanner(mainTable, auths);
			scan.setRange(new Range(rowid, rowid));
			scan.fetchColumn(ColFam, ColFamQual);

			for (Entry<Key, Value> entry : scan) {
				physicalHost = entry.getValue().toString();
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + mainTable);
			e.printStackTrace();
		}

		return physicalHost;

	}

	@Override
	public String getType(String SliceID, String ReservationID) {
		Authorizations auths = new Authorizations();
		Text rowid = new Text(createRowId(SliceID, ReservationID));
		Text ColFam = new Text(COMETClientConst.COLFAM_SYSTEM);
		Text ColFamQual = new Text(COMETClientConst.MAIN_COLQUAL_RSRCE_TYPE);
		String type = null;
		try {
			Scanner scan = cometConnector.createScanner(mainTable, auths);
			scan.setRange(new Range(rowid, rowid));
			scan.fetchColumn(ColFam, ColFamQual);

			for (Entry<Key, Value> entry : scan) {
				type = entry.getValue().toString();
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + mainTable);
			e.printStackTrace();
		}

		return type;
	}

	/**
	 * @see @link {@link COMETClientIfce#getScript(String, String)}
	 */
	@Override
	public String getScript(String SliceID, String ReservationID) {
		Authorizations auths = new Authorizations();
		Text rowid = new Text(createRowId(SliceID, ReservationID));
		Text ColFam = new Text(COMETClientConst.COLFAM_USER);
		Text ColFamQual = new Text(COMETClientConst.MAIN_COLQUAL_SCRIPT);
		String script = null;
		try {
			Scanner scan = cometConnector.createScanner(mainTable, auths);
			scan.setRange(new Range(rowid, rowid));
			scan.fetchColumn(ColFam, ColFamQual);

			for (Entry<Key, Value> entry : scan) {
				script = entry.getValue().toString();
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + mainTable);
			e.printStackTrace();
		}

		return script;
	}

	@Override
	public String scanData() {

		Authorizations auths = new Authorizations();
		try {
			Scanner scan = cometConnector.createScanner(mainTable, auths);
			for (Entry<Key, Value> entry : scan) {

				Text rowid = entry.getKey().getRow();
				System.out.println("Rowid: " + rowid + " ColumnFamily: "
						+ entry.getKey().getColumnFamily()
						+ " ColumnQualifier: "
						+ entry.getKey().getColumnQualifier() + " Value: "
						+ entry.getValue());

				Scanner scanIfces = cometConnector.createScanner(
						interfaceTable, auths);
				scanIfces.setRange(Range.prefix(rowid));
				for (Entry<Key, Value> entry2 : scanIfces) {
					System.out.println("Rowid: " + entry2.getKey().getRow()
							+ " ColumnFamily: "
							+ entry2.getKey().getColumnFamily()
							+ " ColumnQualifier: "
							+ entry2.getKey().getColumnQualifier() + " Value: "
							+ entry2.getValue());

				}
			}

		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + mainTable);
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String getIfces(String SliceID, String ReservationID) {

		Authorizations auths = new Authorizations();
		Text rowid = createRowId(SliceID, ReservationID);
		try {
			Scanner scan2 = cometConnector.createScanner(interfaceTable, auths);
			scan2.setRange(Range.prefix(rowid));
			for (Entry<Key, Value> entry : scan2) {
				System.out.println(entry.getKey().getRow() + " "
						+ entry.getKey().getColumnFamily() + " "
						+ entry.getKey().getColumnQualifier() + "  "
						+ entry.getValue());
			}
			scan2.close();
		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + mainTable);
			e.printStackTrace();
		}
		return accumuloInstance;
	}

	@Override
	public String getIfceData(String SliceID, String ReservationID, String Ifce) {

		Authorizations auth = new Authorizations();
		Text rowid = createRowId(SliceID, ReservationID);
		String suffix = ":" + Ifce;
		suffix.trim();
		rowid.append(suffix.getBytes(), 0, suffix.length());
		try {
			Scanner scan = cometConnector.createScanner(interfaceTable, auth);
			scan.setRange(new Range(rowid, rowid));
			for (Entry<Key, Value> entry : scan) {
				System.out.println("Rowid: " + entry.getKey().getRow()
						+ " ColumnFamily: " + entry.getKey().getColumnFamily()
						+ " ColumnQualifier: "
						+ entry.getKey().getColumnQualifier() + " Value: "
						+ entry.getValue());
			}
		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + interfaceTable);
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String getIfceIp(String SliceID, String ReservationID, String Ifce) {
		Authorizations auth = new Authorizations();
		Text rowid = createRowId(SliceID, ReservationID);
		String suffix = ":" + Ifce;
		suffix.trim();
		rowid.append(suffix.getBytes(), 0, suffix.length());

		Scanner scan;
		try {
			scan = cometConnector.createScanner(interfaceTable, auth);
			scan.setRange(new Range(rowid, rowid));
			scan.fetchColumn(new Text(COMETClientConst.INTERFACES_COLFAM),
					new Text(COMETClientConst.INTERFACES_COLQUAL_IP));
			for (Entry<Key, Value> entry : scan) {
				System.out.println("Rowid: " + entry.getKey().getRow()
						+ " ColumnFamily: " + entry.getKey().getColumnFamily()
						+ " ColumnQualifier: "
						+ entry.getKey().getColumnQualifier() + " Value: "
						+ entry.getValue());
			}
		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + interfaceTable);
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public String getIfceState(String SliceID, String ReservationID, String Ifce) {
		Authorizations auth = new Authorizations();
		Text rowid = createRowId(SliceID, ReservationID);
		String suffix = ":" + Ifce;
		suffix.trim();
		rowid.append(suffix.getBytes(), 0, suffix.length());

		Scanner scan;
		try {
			scan = cometConnector.createScanner(interfaceTable, auth);
			scan.setRange(new Range(rowid, rowid));
			scan.fetchColumn(new Text(COMETClientConst.INTERFACES_COLFAM),
					new Text(COMETClientConst.INTERFACES_COLQUAL_STATE));
			for (Entry<Key, Value> entry : scan) {
				System.out.println("Rowid: " + entry.getKey().getRow()
						+ " ColumnFamily: " + entry.getKey().getColumnFamily()
						+ " ColumnQualifier: "
						+ entry.getKey().getColumnQualifier() + " Value: "
						+ entry.getValue());
			}
		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + interfaceTable);
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public String getIfceProtocol(String SliceID, String ReservationID,
			String Ifce) {
		Authorizations auth = new Authorizations();
		Text rowid = createRowId(SliceID, ReservationID);
		String suffix = ":" + Ifce;
		suffix.trim();
		rowid.append(suffix.getBytes(), 0, suffix.length());

		Scanner scan;
		try {
			scan = cometConnector.createScanner(interfaceTable, auth);
			scan.setRange(new Range(rowid, rowid));
			scan.fetchColumn(new Text(COMETClientConst.INTERFACES_COLFAM),
					new Text(COMETClientConst.INTERFACES_COLQUAL_PROT));
			for (Entry<Key, Value> entry : scan) {
				System.out.println("Rowid: " + entry.getKey().getRow()
						+ " ColumnFamily: " + entry.getKey().getColumnFamily()
						+ " ColumnQualifier: "
						+ entry.getKey().getColumnQualifier() + " Value: "
						+ entry.getValue());
			}
		} catch (TableNotFoundException e) {
			log.error("Unable to find table " + interfaceTable);
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void deleteEntry(String SliceID, String ReservationID) {

		Authorizations auth = new Authorizations();
		Text rowid = createRowId(SliceID, ReservationID);

		try {
			Scanner scan = cometConnector.createScanner(mainTable, auth);
			scan.setRange(new Range(rowid, rowid));

			Scanner scan2 = cometConnector.createScanner(interfaceTable, auth);
			scan2.setRange(Range.prefix(rowid));

			deleteRow(scan, mainTable);
			deleteRow(scan2, interfaceTable);

		} catch (TableNotFoundException e) {
			log.error("DeleteEntry failed due to: " + e.getMessage());
		}

	}

	private void deleteRow(Scanner scanner, String table) {
		Mutation deleter = null;
		for (Entry<Key, Value> entry : scanner) {
			if (deleter == null) {
				deleter = new Mutation(entry.getKey().getRow());
			}
			deleter.putDelete(entry.getKey().getColumnFamily(), entry.getKey()
					.getColumnQualifier());

		}
		try {
			BatchWriter bw = createBatchWriter(table);
			bw.addMutation(deleter);
			bw.close();
		} catch (MutationsRejectedException e) {
			log.error("Error deleting record. Reason: " + e.getMessage());
		}
	}


	@Override
	public String getAllData(String SliceID, String ReservationID) {
		Authorizations auths = new Authorizations();
		Text rowid = createRowId(SliceID, ReservationID);
		try {
			Scanner scan2 = cometConnector.createScanner(mainTable, auths);
			scan2.setRange(new Range(rowid,rowid));
			for (Entry<Key, Value> entry : scan2) {
				System.out.print(entry.getKey().getRow() + " ");
				System.out.println(entry.getKey().getColumnFamily() + " "
						+ entry.getKey().getColumnQualifier() + "  "
						+ entry.getValue());
			}
			scan2.close();
		} catch (TableNotFoundException e) {
			log.error("Table not found. Reason: " + e.getMessage());	
		}
	
		
		try {
			Scanner scan2 = cometConnector.createScanner(interfaceTable, auths);
			scan2.setRange(new Range(rowid,rowid));
			for (Entry<Key, Value> entry : scan2) {
				System.out.print(entry.getKey().getRow() + " ");
				System.out.println(entry.getKey().getColumnFamily() + " "
						+ entry.getKey().getColumnQualifier() + "  "
						+ entry.getValue());
			}
			scan2.close();
		} catch (TableNotFoundException e) {
			log.error("Table not found. Reason: " + e.getMessage());	
		}

		return null;
	}
	

	
}
