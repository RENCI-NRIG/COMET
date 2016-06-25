
/*
 * Copyright (c) 2016 RENCI/UNC Chapel Hill 
 *
 * @author Claris Castillo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software 
 * and/or hardware specification (the "Work") to deal in the Work without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Work, and to permit persons to whom the Work is furnished to do so, subject to 
 * the following conditions:  
 * The above copyright notice and this permission notice shall be included in all copies or 
 * substantial portions of the Work.  
 *
 * THE WORK IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
 * OUT OF OR IN CONNECTION WITH THE WORK OR THE USE OR OTHER DEALINGS 
 * IN THE WORK.
 */

package comet.accumulo.genc;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
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
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.SecurityOperationsImpl;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import comet.accumulo.genc.utils.COMETClientUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Accumulo-specific implementation  of General Client Interface for COMET
 * @author claris
 * TODO Maintain an internal data-structure with the mapping of ContextSubType and {authorization labels} 
 * TODO Configure the client settings, membbuffer and numberOfThreads. Currently using the default values.
 * TODO Organize private methods. Put at the end of the class. Reconsider methods that should go into the {@link comet.accumulo.genc.utils}
 * TODO Global property class to define String tokens ane labels. This is particularly important for labels used in JSON responses to client.
 * 
 */

public class COMETGenCImpl implements COMETGenCIfce {
	public  static String ERROR = "error";
	private static final Logger log = Logger.getLogger(COMETGenCImpl.class);
	String configFile;
	Connector cometConnector;
	ClientConfiguration clientConf;
	String username;
	String targetTable;
	String sliceTable;
	String reservationTable;
	String principalsTable;
	String root;
	String rootPassword;
	String labelProvider;
	String labelUser;
	String FamQualIaas;
	String FamQualUser;



	@SuppressWarnings({ "deprecation"})
	public void init(String username, String password)
			throws AccumuloException, AccumuloSecurityException
			 {

		Properties props = null;
		this.username=username;

		final String keystore;
		final String truststore;
		final String keystorePass;
		final String truststorePass;
		final String accumuloInstance;
		final String zookeeperHosts;
		@SuppressWarnings("unused")
		final Long memBuffer;
		@SuppressWarnings("unused")
		final int numberOfThreads;



		try {
			props = COMETClientUtils.getClientConfigProps(configFile);
		} catch (IOException e1) {
			log.error("Error initializing accumulo client. Message: " + e1.getMessage());
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
		sliceTable = props
				.getProperty(ClientConfigProps.COMET_SERVICE_ACCUMULO_TABLE_SLICES);
		reservationTable = 
				props
				.getProperty(ClientConfigProps.COMET_SERVICE_ACCUMULO_TABLE_RESERVATIONS);
		principalsTable = 
				props
				.getProperty(ClientConfigProps.COMET_SERVICE_ACCUMULO_TABLE_PRINCIPALS);
		root = props.getProperty(ClientConfigProps.COMET_SERVICE_ACCUMULO_AUTH_ROOT);
		rootPassword = props.getProperty(ClientConfigProps.COMET_SERVICE_ACCUMULO_AUTH_ROOTPASSWORD);
		labelProvider = props.getProperty(ClientConfigProps.COMET_SERVICE_ACCUMULO_AUTH_LABEL_ACTOR);
		labelUser = props.getProperty(ClientConfigProps.COMET_SERVICE_ACCUMULO_AUTH_LABEL_USER);
		FamQualIaas=props.getProperty(ClientConfigProps.COMET_SERVICE_ACCUMULO_DATASCHEMA_IAAS);
		FamQualUser = props.getProperty(ClientConfigProps.COMET_SERVICE_ACCUMULO_DATASCHEMA_USER);



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
		clientConf = new ClientConfiguration();
		clientConf.withSsl(true);
		clientConf.withKeystore(keystore, keystorePass, "JKS");
		clientConf.withTruststore(truststore, truststorePass, "JKS");
		clientConf.withInstance(accumuloInstance);
		clientConf.withZkHosts(zookeeperHosts);

		// Accumulo instance creation
		log.setLevel(Level.DEBUG);

		/*the instance can be obtained through the cometConnector*/
		Instance instance = new ZooKeeperInstance(clientConf);

		cometConnector = instance.getConnector(username, password);

	}

	public COMETGenCImpl(String configurationFile) {
		/*Configuration property file*/
		configFile = configurationFile;
	}
	
	public Connector getConnector() {
		return this.cometConnector;
	}
	
	public ClientConfiguration getClientConfig() {
		return this.clientConf;
	}

	public String deleteAllRowsInTable(String contextType) {

		TableOperations tableOps = cometConnector.tableOperations();
		try {
			tableOps.deleteRows(contextType,null, null);
		} catch (AccumuloException | AccumuloSecurityException
				| TableNotFoundException e) {
			log.error("Table contextType " + contextType + " not found.");
			e.printStackTrace();
		}
		return "Table contextType deleted: " + contextType + "\n";
	}
	
	/**
	 * TODO Change Scanner to BatchScanner to improve efficiency
	 */
	public JSONObject enumerateAllInTable(String contextType, String visibility, String numberOfThreads) {

	
		JSONObject output = new JSONObject();
		Authorizations auth = null;
		
		if (visibility != null) {
			auth = new Authorizations(visibility);
		} else {
			auth = new Authorizations();
		}

		try {
			Scanner scan = cometConnector.createScanner(contextType, auth);
			scan.setRange(new Range());

			  Iterator<Map.Entry<Key,Value>> iterator = scan.iterator();
			  
			  while (iterator.hasNext()) {
				 
			   Map.Entry<Key,Value> entry = iterator.next();
			   Key key2 = entry.getKey();
			   Value value = entry.getValue();
			   output.put(key2.toString(), value);
			//   output.append(key2.toString(), value);
			//  System.out.println(key2 + " ==> " + value);
			  }

		} catch (TableNotFoundException e) {
			log.error("enumerateAll failed due to: " + e.getMessage());
			try {
				return output.put(ERROR, "Failed to delete entry due to " + e.getMessage());
			} catch (JSONException e1) {
				log.error("JSON Exception: " + e1.getMessage());
			}

		} catch (Exception e) {
			log.error("enumerateAll failed due to: " + e.getMessage());
			try {
				return output.put(ERROR, "Failed to delete entry due to " + e.getMessage());
			} catch (JSONException e1) {
				log.error("JSON Exception: " + e1.getMessage());
			}

		}

		
		return output;
	}
	
		@SuppressWarnings("deprecation")
	public Set<String> enumarateUsers() {
		Credentials credentials = new Credentials(root,new PasswordToken(rootPassword.getBytes()));

		HashSet<String> set = new HashSet<String>();
		ClientContext clientContext = new ClientContext(cometConnector.getInstance(), 
				credentials, clientConf);
		SecurityOperationsImpl securityOpImpl = new SecurityOperationsImpl(clientContext);

		try {
			set = (HashSet<String>) securityOpImpl.listUsers();
		} catch (AccumuloException e) {
			log.error("Accumulo Exception "+e.getMessage());
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			log.error("Accumulo Security Exception: " + e.getMessage());
			e.printStackTrace();
		}

		return set;
	}
	@SuppressWarnings({ "deprecation" })
	public String addUser(String username,  String password,
			Authorizations auth) {

		Credentials credentials = new Credentials(root,new PasswordToken(rootPassword.getBytes()));

		ClientContext clientContext = new ClientContext(cometConnector.getInstance(), 
				credentials, clientConf);
		SecurityOperationsImpl securityOpImpl = new SecurityOperationsImpl(clientContext);
		try {
			securityOpImpl.createUser(username,password.getBytes(), auth);
		} catch (AccumuloException | AccumuloSecurityException e) {
			log.error("Accumulo Security Exception: " + e.getMessage());
			e.printStackTrace();
		}
		return "User " + username + " has been created.";
	}

	public String removeUserAuthorizations(String username, String authToRemove) {

		List<ByteBuffer> newAuth = new ArrayList<ByteBuffer>();
		List<ByteBuffer> visLabels = getVisibilityLabelByteBuffer(username);

		/**Process the labels. Read existing ones and add new ones. **/

		for (ByteBuffer byteBuffer : visLabels) {

			String label=null;
			try {
				label = new String(byteBuffer.array(), "UTF-8");
				label.trim();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}

			if(label.contains(authToRemove)) {
				continue;
			}

			newAuth.add(byteBuffer);
		}

		Authorizations newAuthorizations = new Authorizations(newAuth);


		ClientContext clientContext = 
				new ClientContext(cometConnector.getInstance(), new Credentials(root, new PasswordToken(rootPassword.getBytes())), clientConf);
		SecurityOperationsImpl securityOpImpl = new SecurityOperationsImpl(clientContext);
		try {
			securityOpImpl.changeUserAuthorizations(username, newAuthorizations);
		} catch (AccumuloException e) {
			log.error("Accumulo Exception: " + e.getMessage());
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			log.error("Accumulo Security Exception: " + e.getMessage());
			e.printStackTrace();
		}

		return null;
	}
	public String addUserAuthorizations(String username, ArrayList<String> auths) {

		List<ByteBuffer> newAuth = new ArrayList<ByteBuffer>();
		List<ByteBuffer> visLabels = getVisibilityLabelByteBuffer(username);

		/**Process the labels. Read existing ones and add new ones. **/
		for (ByteBuffer byteBuffer : visLabels) {
			newAuth.add(byteBuffer);	
		}

		for(String string : auths) {
			try {
				byte[] b = string.getBytes("UTF-8");
				ByteBuffer bb = ByteBuffer.wrap(b);
				newAuth.add(bb);
			} catch (UnsupportedEncodingException e) {
				log.error("Unsupported encoding exception: " + e.getMessage());
			}

		}
		Authorizations newAuthorizations = new Authorizations(newAuth);

		ClientContext clientContext = 
				new ClientContext(cometConnector.getInstance(), new Credentials(root, new PasswordToken(rootPassword.getBytes())), clientConf);
		SecurityOperationsImpl securityOpImpl = new SecurityOperationsImpl(clientContext);
		try {

			securityOpImpl.changeUserAuthorizations(username, newAuthorizations);
		} catch (AccumuloException e) {
			log.error("Accumulo Exception: " + e.getMessage());
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			log.error("Accumulo Security Exception: " + e.getMessage());
			e.printStackTrace();
		}



		return null;
	}

	public List<ByteBuffer> getVisibilityLabelByteBuffer(String username) {

		ClientContext clientContext = 
				new ClientContext(cometConnector.getInstance(), new Credentials(root, new PasswordToken(rootPassword.getBytes())), clientConf);
		SecurityOperationsImpl securityOpImpl = new SecurityOperationsImpl(clientContext);
		Authorizations auth=new Authorizations();

		try {
			auth = securityOpImpl.getUserAuthorizations(username);
			/*returns encoded in UTF-8*/
			return auth.getAuthorizationsBB();

		} catch (AccumuloException e) {
			log.error("Accumulo Exception: "+e.getMessage());
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			log.error("Accumulo Security Exception: "+ e.getMessage());
			e.printStackTrace();
		}
		return auth.getAuthorizationsBB();
	}
	public List<byte[]> getVisibilityLabelsListBytes(String username) {
		ClientContext clientContext = new ClientContext(cometConnector.getInstance(), new Credentials(root, new PasswordToken(rootPassword.getBytes())), clientConf);
		SecurityOperationsImpl securityOpImpl = new SecurityOperationsImpl(clientContext);

		List<byte[]> authLabels=new ArrayList<byte[]>();
		try {
			Authorizations auth = securityOpImpl.getUserAuthorizations(username);
			authLabels= auth.getAuthorizations();
		} catch (AccumuloException | AccumuloSecurityException e) {
			log.error("Accumulo Security Exception: " + e.getMessage());
			e.printStackTrace();
		} 

		return authLabels;
	}


	/**
	 * Private method to validate ReadScope call. 
	 * if it is slice table
			{
			if the baseVisLabel is actor, the caller must be user or actor
			 if the baseVislabel is user, the caller must be user
			 }

		if it is a reservation table
		    { 
		    if the baseVisLabel is actor, the caller must be user or actor
		 	if the baseVisLabel is user, nothign is possible.
		 	}
	 * @param username
	 * @param contextType
	 * @param contextSubType
	 * @param baseContextType
	 * @param baseContextSubType
	 * @param baseVisLabel
	 * @return false if the FamQualIaas is not user nor actor or the user is not allowed to perform the operation --as per the logic.
	 */
	public boolean validateReadScope(String username, String contextType, 
			String contextSubType) 
	{

		boolean validated = false;
		String baseVisLabel=null;
		if(contextSubType.equals(FamQualIaas)) {
			baseVisLabel = labelProvider;

		}  else if (contextSubType.equals(FamQualUser)) {

			baseVisLabel = labelUser;
		} else {

			return validated;
		}

		COMETAdminImpl adminOps = new COMETAdminImpl(root, rootPassword, cometConnector, clientConf);
		List<ByteBuffer> list = adminOps.getVisibilityLabelByteBufferList(username);
		try {
			byte[] b = labelUser.getBytes("UTF-8");
			byte[] b2 = labelProvider.getBytes("UTF-8");

			if(contextSubType.equals(FamQualIaas)) {

				for (ByteBuffer byteBuffer : list) {


					if(byteBuffer.compareTo(ByteBuffer.wrap(b))==0 || byteBuffer.compareTo(ByteBuffer.wrap(b2))==0)
					{
						validated = true;
						return validated;
					}
				}

			}  else if (contextSubType.equals(FamQualUser)) {

				if(contextType.equalsIgnoreCase(reservationTable)) { //this is only if the table is reservations

					for (ByteBuffer byteBuffer : list) {


						if(byteBuffer.compareTo(ByteBuffer.wrap(b))==0)
						{
							validated = true;
							return validated;
						}

					}
				}
			} else {
				return validated;
			}

		} catch (UnsupportedEncodingException e) {
			log.error("Unsupported Encoding Exception "+e.getMessage());
		}
		return validated;
	}

	/**
	 * Private method to validate modifyScope operation.  
	 * For both tables (slice and reservations) there must be a match between FamQUal (User or Iaas) and Authorization label (user and actor)
	 * @param username
	 * @param contextType
	 * @param contextSubType
	 * @return
	 * TODO This call is identical to validateReadScope. We should not merge because the logics may change independently.
	 */
	public boolean validateModifyScope(String username, String contextType, String contextSubType) {

		String baseVisLabel;
		boolean validated = false;

		if(contextSubType.equals(FamQualIaas)) {
			baseVisLabel = labelProvider;

		}  else if (contextSubType.equals(FamQualUser)) {
			baseVisLabel = labelUser;
		} else {	
			return validated;
		}
		COMETAdminImpl adminOps = new COMETAdminImpl(root, rootPassword, cometConnector, clientConf);
		List<ByteBuffer> list = adminOps.getVisibilityLabelByteBufferList(username);

		for (ByteBuffer byteBuffer : list) {
			try {
				byte[] b = baseVisLabel.getBytes("UTF-8");
				if(byteBuffer.compareTo(ByteBuffer.wrap(b))==0)
				{
					validated = true;
					return validated;
				}
			} catch (UnsupportedEncodingException e) {
				log.error("Unssuported Encoding: " + e.getMessage());
				e.printStackTrace();
			}
		}
		return validated;
	}
	/**
	 * Private method to validate createScope operation. 
	 * For both tables (slice and reservations) there must be a match between FamQUal (User or Iaas) and Authorization label (user and actor)
	 * @param username
	 * @param contextType
	 * @param contextSubType
	 * @return true if valid, false otherwise
	 */
	public boolean validateCreateScope(String username, String contextType, String contextSubType) 
	{

		String baseVisLabel;
		boolean validated = false;


		if(contextSubType.equals(FamQualIaas)) {
			baseVisLabel = labelProvider;

		}  else if (contextSubType.equals(FamQualUser)) {

			baseVisLabel = labelUser;
		} else {

			return validated;
		}

		COMETAdminImpl adminOps = new COMETAdminImpl(root, rootPassword, cometConnector, clientConf);
		List<ByteBuffer> list = adminOps.getVisibilityLabelByteBufferList(username);

		for (ByteBuffer byteBuffer : list) {
			try {
				byte[] b = baseVisLabel.getBytes("UTF-8");
				if(byteBuffer.compareTo(ByteBuffer.wrap(b))==0)
				{
					validated = true;
					return validated;
				}
			} catch (UnsupportedEncodingException e) {
				log.error("Unssuported Encoding: " + e.getMessage());
				e.printStackTrace();
			}
		}

		return validated;
	}
	/**
	 * CreateScope
	 * @see {@link comet.accumulo.genc.COMETGenCIfce#createScope(String, String, String, String, String, String)}
	 */
	public JSONObject createScope(String contextType, String contextSubType,
			String contextID, String scopeName, String scopeValue,
			String visibility) {
		JSONObject out = new JSONObject();
		if(!validateCreateScope(username, contextType, contextSubType)) {
			try {
				out.put(ERROR, "Operation not allowed by user " + username);
			} catch (JSONException e) {
				log.error("JSON Exception: " + e.getMessage());
			}
			return out;
			//return new String("Operation not allowed by user " + username);
		} 
		BatchWriter bw = null;
		Mutation mut1 = new Mutation(new Text(contextID));
		Text colFam2 = new Text(contextSubType);
		Text ColFam2ColQual1 = new Text(scopeName);
		if (visibility != null) {
			mut1.put(colFam2, ColFam2ColQual1,
					new ColumnVisibility(visibility),
					new Value(scopeValue.getBytes()));
		} else {
			mut1.put(colFam2, ColFam2ColQual1, new Value(scopeValue.getBytes()));
		}

		try {
			bw = createBatchWriter(contextType);
			bw.addMutation(mut1);
			bw.close(); // flushes and release ---no need for bw.flush()


		} catch (MutationsRejectedException e) {
			log.error("Error: Failed mutation in creating new entry ("
					+ contextID + ")");
			e.printStackTrace();
			try {
				out.put(ERROR, "Failed to create new scope entry");
			} catch (JSONException e1) {
				log.error("JSON Exception: " + e1.getMessage());
			}
			return out;
			//	return "Error: Failed to create new scope entry";
		} catch (Exception e) {
			try {
				out.put(ERROR, "Failes to create new scope entry");
			} catch (JSONException e1) {
				log.error("JSON Exception: " + e1.getMessage());
			}
			System.out.println("OUTPUT IS " + out.toString());
			return out;
			//return "Failed to create new scope entry";
		}


		/*if it is a new slice, add a new user */
		if(contextType.equalsIgnoreCase(sliceTable)) {
			addUser("userdn-"+ contextID, "password", new Authorizations(contextID));
		}


		try {

			out.put("contextID",contextID);
			System.out.println("ABOUT TO OUTPUT SOMETHIN "+out.toString());
		} catch (JSONException e) {
			log.error("JSON Exception: " + e.getMessage());
		}
		return out;
	}

	/**
	 * @see {@link comet.accumulo.genc.COMETGenCIfce#destroyScope(String, String, String, String, String)}
	 * @TODO: If deleterow returns a null we have to return an empty string.
	 *        JSON error messages
	 */
	public JSONObject destroyScope(String contextType, String contextSubType,
			String contextID, String scopeName, String visibility) {

		JSONObject output = new JSONObject();
		Authorizations auth = null;

		if (visibility != null) {
			auth = new Authorizations(visibility);

		} else {
			auth = new Authorizations();
		}

		String key = null;


		try {
			Scanner scan = cometConnector.createScanner(contextType, auth);
			scan.setRange(new Range(contextID, contextID));


			key = deleteRow(scan, contextType, visibility, contextSubType, scopeName);


		} catch (TableNotFoundException e) {
			log.error("DeleteEntry failed due to: " + e.getMessage());
			try {
				return output.put(ERROR, "Failed to delete entry due to " + e.getMessage());
			} catch (JSONException e1) {
				log.error("JSON Exception: " + e1.getMessage());
			}

		} catch (Exception e) {
			log.error("DeleteEntry failed due to: " + e.getMessage());
			try {
				return output.put(ERROR, "Failed to delete entry due to " + e.getMessage());
			} catch (JSONException e1) {
				log.error("JSON Exception: " + e1.getMessage());
			}

		}

		try {
			output.put("contextID",key);
		} catch (JSONException e) {
			log.error("JSON Exception: " + e.getMessage());
		}
		return output;
	}

	private String deleteRow(Scanner scanner, String table, String visibility, String contextSubType, String scopeName) {
		Mutation deleter = null;
		for (Entry<Key, Value> entry : scanner) {

			if (deleter == null) {
				deleter = new Mutation(entry.getKey().getRow());

			}
			if (visibility != null) {
				deleter.putDelete(new Text(contextSubType), new Text(scopeName), new ColumnVisibility(visibility));

			} else {

				deleter.putDelete(entry.getKey().getColumnFamily(), entry
						.getKey().getColumnQualifier());
			}

		}
		try {
			if (deleter != null) {

				BatchWriter bw = createBatchWriter(table);
				bw.addMutation(deleter);
				bw.close();

			}

		} catch (MutationsRejectedException e) {
			log.error("Error deleting record. Reason: " + e.getMessage());
			return "Error: Failed to delete scope.\n";
		}

		if (deleter != null) {
			return "ContextID: " + deleter.getRow().toString() + '\n';
		}

		return "Error: No scope to delete. \n";
	}

	/**
	 * ReadScope
	 * @see {@link comet.accumulo.genc.COMETGenCIfce#readScope(String, String, String, String, String)}
	 */
	public JSONObject readScope(String contextType, String contextSubType,
			String contextID, String scopeName, String visibility) {
		JSONObject output = new JSONObject();
		if(!(validateReadScope(username, contextType, contextSubType)
				||
				validateReadScope(username, contextType, contextSubType)
				|| 
				validateReadScope(username, contextType, contextSubType)
				))
		{



			try {

				return output.put(ERROR, new String("Operation not allowed by user" ));
			} catch (JSONException e) {
				log.error("JSON Exception: " + e.getMessage());
			}
		}
		Authorizations auths = new Authorizations(visibility);
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

			try {
				return output.put(ERROR, "Table not found.");
			} catch (JSONException e1) {
				log.error("JSON Exception: " + e1.getMessage());
			}
		} catch (Exception e) {
			try {
				return output.put(ERROR,"Failed to read scope.");
			} catch (JSONException e1) {
				log.error("JSON Exception: " + e1.getMessage());

			}
		}
		try {

			output.put(scopeName, scopeValue);
		} catch (JSONException e) {
			log.error("JSON Exception: " + e.getMessage());

		}

		return output;

	}

	/**
	 * ModifyScope
	 * @see {@link comet.accumulo.genc.COMETGenCIfce#modifyScope(String, String, String, String, String, String)}
	 */
	public JSONObject modifyScope(String contextType, String contextSubType,
			String contextID, String scopeName, String newScopeValue,
			String visibility) {
		JSONObject output=new JSONObject();
		BatchWriter bw = null;
		Mutation mut1 = new Mutation(new Text(contextID));
		Text colFam2 = new Text(contextSubType);
		Text ColFam2ColQual1 = new Text(scopeName);
		if(visibility!=null) {
			mut1.put(colFam2, ColFam2ColQual1, new ColumnVisibility(visibility),new Value(newScopeValue.getBytes()));	
		} else {
			mut1.put(colFam2, ColFam2ColQual1,new Value(newScopeValue.getBytes()));
		}
		try {
			bw = createBatchWriter(contextType);
			bw.addMutation(mut1);
			bw.close(); // flushes and release ---no need for bw.flush()
		} catch (MutationsRejectedException e) {
			log.error("Failed mutation in updating  entry (" + contextID + ")");
			e.printStackTrace();
			try {
				return output.put(ERROR, "Failed to modify scope");
			} catch (JSONException e1) {
				log.error("JSON Exception: " + e1.getMessage());

			}

		} catch (Exception e) {
			log.error("Failed mutation in updating  entry (" + contextID + ")");
			try {
				return output.put(ERROR,"Failed to modify scope");
			} catch (JSONException e1) {
				log.error("JSON Exception: " + e1.getMessage());

			}
		}

		try {
			output.put("contextID", contextID);
			output.put(scopeName, newScopeValue);
		} catch (JSONException e) {
			log.error("JSON Exception: " + e.getMessage());
		}		
		return output;

	}

	/* (non-Javadoc)
	 * @see comet.accumulo.genc.COMETGenCIfce#enumerateScopes(java.lang.String, java.lang.String, java.lang.String)
	 * TODO I have serious concerns about the implementation of this call. First there is the issue that I am adding strings to 
	 * a Text object and then that the Text object is stringized to be returned.
	 */
	@Override
	public JSONObject enumerateScopes(String username, String contextType, String contextSubType, String contextID, String visibility) {

		//Scan method in accumulo with proper auuthorization labels
		JSONObject output = new JSONObject();
		Authorizations auth = null;

		if (visibility != null) {
			auth = new Authorizations(visibility);

		} else {
			auth = new Authorizations();
		}



		try {
			Scanner scanner = cometConnector.createScanner(contextType, auth);
			scanner.setRange(new Range(contextID,contextID)); //constructor empty implies the whole table (startKey=-neg, endKey=-pos)

			for (Entry<Key, Value> entry : scanner) {


				if(entry.getKey().compareColumnFamily(new Text(contextSubType))==0) {

					Value v = entry.getValue();
					Text valueText = new Text(v.get());
					Key k = entry.getKey();
					Text scopeName = k.getColumnQualifier();
					output.put(scopeName.toString(), valueText.toString());

				}
			}
		} catch (TableNotFoundException e) {
			log.error("DeleteEntry failed due to: " + e.getMessage());
			try {
				return output.put(ERROR, "Failed to delete entry due to " + e.getMessage());
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		} catch (Exception e) {
			log.error("DeleteEntry failed due to: " + e.getMessage());
			try {
				return output.put(ERROR, "Failed to delete entry due to " + e.getMessage());
			} catch (JSONException e1) {
				log.error("Json Exception: "  + e1.getMessage());
			}
		}
		return output;

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

		}

		return bw;
	}

	public static void main(String args[]) {
		String sliceid = UUID.randomUUID().toString();
		String scopeid = UUID.randomUUID().toString();
		String scopevalue = UUID.randomUUID().toString();

		COMETGenCImpl cimpl = new COMETGenCImpl(
				"/Users/clariscastillo/Documents/development/comet/tomcat-ssl/configFile");

		try {
			cimpl.init("pruth", "pruthc4m2t");
		//	cimpl.init("root", "accumuloAuth");

		} catch (Exception e) {
			log.error("Exception: "+e.getMessage());
			e.printStackTrace();
		}

		COMETAdminImpl adminOps = new COMETAdminImpl("root", "accumuloAuth", cimpl.getConnector(), cimpl.clientConf);
		String x= adminOps.listUsers().toString();
		System.out.println(x);
		JsonReader jsonr = Json.createReader(new StringReader(x));
		JsonObject object = jsonr.readObject();
		Collection<JsonValue> values = object.values();
		for (JsonValue jsonValue : values) {
			String theUser = jsonValue.toString();
		//	System.out.println("jsonValue " + theUser );
			if(theUser.contains("userdn")) 
			{
				
				System.out.println(theUser);
				System.out.println(theUser.charAt(0));
			//	adminOps.removeUser("userdn-a7090740-89f4-4e32-a31c-a4f514e7c078");
			} else {
				System.out.println("NO CONTIENTE " + theUser);
			}
		}
		

	//	 cimpl.enumerateAllInTable("virtualsystems", "actor", "10");
			//	cimpl.enumerateScopes("pruth", "virtualsystems", "iaas", "a3d6e3fc-1fc1-497b-95bc-0e9bc8fea947", "secret");

		
				System.exit(1);
		if(cimpl.validateReadScope("pruth", "virtualsystems", "iaas"))
			System.out.println("TRUE");
		else System.out.println("FALSE");

		/*if(cimpl.validateCreateScope("pruth", "virtualsystems", "iaas"))
			System.out.println("TRUE");
		else System.out.println("FALSE");*/
		System.exit(1);

		/*
		ArrayList<String> array = new ArrayList<String>();
		array.add("secret");
		cimpl.addUserAuthorizations("pruth",array);
	//	System.exit(1);
		 */		
		/******/

		//		cimpl.removeUserAuthorizations("pruth", "secret");
		//		System.out.println("DONE REMOVING ");

		Set<String> users = cimpl.enumarateUsers();
		for (String string : users) {
			System.out.println("User "+ string);
		}

		//	cimpl.removeUserAuthorizations("pruth", "actor");
		//	System.exit(10);

		ArrayList<String> listOf = new ArrayList<String>();
		listOf.add(new String("actor"));

		//	cimpl.addUserAuthorizations("pruth", listOf);
		//	System.out.println("DONE"); System.exit(10);

		System.out.println("About to call getVisibilityLabels " );
		List<ByteBuffer> mylist = cimpl.getVisibilityLabelByteBuffer("pruth");
		for (ByteBuffer byteBuffer : mylist) {
			System.out.println(" A " + new String(byteBuffer.array()));
		}
		System.exit(1);
		cimpl.validateCreateScope("pruth", "virtualsystems", "iaas");


		System.out.println("Again");
		List<byte[]> mylist2 = cimpl.getVisibilityLabelsListBytes("pruth");
		for (byte[] bs : mylist2) {
			System.out.println(new String(bs));
		}

		System.out.println("done calling getVisibilityLabels");

		System.exit(1);
		System.out.println("cratescope");
		cimpl.createScope("virtualsystems", "iaas", sliceid, scopeid,
				scopevalue, null);
		System.out.println("destroyscope");
		cimpl.destroyScope("virtualsystems", "iaas",
				"0f6fde07-90df-4128-97a3-9b983c552d0f",
				"60d311eb-afee-4513-9251-5e3cca21fe77", null);
		System.out.println("updateScope");
		cimpl.modifyScope("virtualsystems", "iaas",
				"e0b088d5-69d3-42f2-b143-5a63574c05b0",
				"03542ec9-9669-43da-ad03-01baa8b26b6a", "newScopeValue++", null);

		System.out.println("readScope");
		System.out.println("ScopeValue is: "
				+ cimpl.readScope("virtualsystems", "iaas",
						"e0b088d5-69d3-42f2-b143-5a63574c05b0",
						"03542ec9-9669-43da-ad03-01baa8b26b6a", null));
	}

	class NonExistentRecords extends Exception {

		private static final long serialVersionUID = -4940370878206181637L;

		public NonExistentRecords(String msg) {
			super(msg);
		}
	}



}
