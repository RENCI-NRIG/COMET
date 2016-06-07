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

/**
 * Accumulo-specific implementation of COMET admin interface {@see comet.accumulo.genc.COMETAdminIfce adminInterface}
 * @author claris
 */
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.SecurityOperationsImpl;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;


public class COMETAdminImpl implements COMETAdminIfce{
	private static final Logger log = Logger.getLogger(COMETAdminImpl.class);
	String root;
	String rootPassword;
	Connector cometConnector;
	ClientConfiguration clientConf;
	ClientContext clientContext;
	Credentials credentials;
	SecurityOperationsImpl securityOpImpl;


	public COMETAdminImpl(String root, String rootPassword, Connector cometConnector, ClientConfiguration clientConf) {
		this.root=root;
		this.rootPassword=rootPassword;
		this.cometConnector=cometConnector;
		this.clientConf=clientConf;
		credentials = new Credentials(root,new PasswordToken(rootPassword.getBytes()));
		clientContext = new ClientContext(cometConnector.getInstance(), 
				credentials, clientConf);
		securityOpImpl = new SecurityOperationsImpl(clientContext);

	}
	@SuppressWarnings("deprecation")
	public String addUser(String username,  String password,
			List<ByteBuffer> labels) {

		Authorizations auth = new Authorizations(labels);
		try {
			securityOpImpl.createUser(username,password.getBytes(), auth);
		} catch (AccumuloException | AccumuloSecurityException e) {
			log.error("Accumulo Security Exception: " + e.getMessage());
			e.printStackTrace();
		}
		return username;
	}

	@Override
	public String setAuthorizations(String username, List<ByteBuffer> authorizations) {


		Authorizations newAuthorizations = new Authorizations(authorizations);

		try {
			securityOpImpl.changeUserAuthorizations(username, newAuthorizations);
		} catch (AccumuloException e) {
			log.error("AccumuloException :" + e.getMessage());
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			log.error("AccumuloSecurityException: " + e.getMessage());
			e.printStackTrace();
		}
		return username;
	}

	@Override
	public String removeUser(String username) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("deprecation")
	@Override
	public Set <String> enumerateUsers() {

		HashSet<String> set = new HashSet<String>();

		try {
			set = (HashSet<String>) securityOpImpl.listUsers();
		} catch (AccumuloException e) {
			log.error("AccumuloException: " + e.getMessage());
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			log.error("Accumulo Security Exception: " + e.getMessage());
			e.printStackTrace();
		}

		return set;
	}

	@Override
	public String removeAuthorization(String username, ByteBuffer authToRemove) {
		List<ByteBuffer> newAuth = new ArrayList<ByteBuffer>();
		List<ByteBuffer> visLabels = getVisibilityLabelByteBuffer(username);

		/**Process the labels. Read existing ones and add new ones. **/

		for (ByteBuffer byteBuffer : visLabels) {

			if(authToRemove.compareTo(byteBuffer)==0){
				continue;
			}
			newAuth.add(byteBuffer);
		}

		Authorizations newAuthorizations = new Authorizations(newAuth);
		/*****************                 				**************/

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

	@Override
	public String addAuthorizations(String username, List<ByteBuffer> labels) {

		List<ByteBuffer> newAuth = new ArrayList<ByteBuffer>();
		List<ByteBuffer> visLabels = getVisibilityLabelByteBuffer(username);

		/**Process the labels. Read existing ones and add new ones. **/
		for (ByteBuffer byteBuffer : visLabels) {
			newAuth.add(byteBuffer);	
		}

		for(ByteBuffer bb : labels) {
			newAuth.add(bb);
		}
		Authorizations newAuthorizations = new Authorizations(newAuth);

		try {
			securityOpImpl.changeUserAuthorizations(username, newAuthorizations);
		} catch (AccumuloException e) {
			log.error("Accumulo Exception: "+ e.getMessage());
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			log.error("Accumulo Security Exception: "+ e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String grantTablePermission(String tableName, String username,
			String permission) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String revokeTablePermission(String tableName, String username,
			String permission) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public List<ByteBuffer> getVisibilityLabelByteBuffer(String username) {
		Authorizations auth=new Authorizations();

		try {
			auth = securityOpImpl.getUserAuthorizations(username);
			/*returns encoded in UTF-8*/
			return auth.getAuthorizationsBB();
		} catch (AccumuloException e) {
			log.error("Accumulo Exception: "+e.getMessage());
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			log.error("Accumulo Security Exception: "+e.getMessage());
			e.printStackTrace();
		}

		return auth.getAuthorizationsBB();
	}




}
