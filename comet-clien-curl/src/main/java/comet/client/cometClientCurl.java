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
package comet.client;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.log4j.Logger;

/**
 * A java-based Restful COMET client written in Jersey
 * @author clariscastillo
 *
 */
public class cometClientCurl {
	
	private static final Logger log = Logger.getLogger(cometClientCurl.class);

	String keyStorePath;
	String keyStorePass;
	String trustStorePath;
	String trustStorePass;
	String baseURL;
	WebTarget webTarget;
	String accumulo_username;
	String accumulo_password;
	/**
	 * Configuration file contains all the parameters required to configure SSL, 
	 * authenticate with COMET and access web resources in COMET
	 * If there is a missing property in the configuration it will return an error. It does not check for empty strings.
	 * @TODO enabled truststore. It is disabled because we don't have yet a CA signed certificate for COMET service.
	 * @param ConfigFile
	 */
	public cometClientCurl(String ConfigFile) {
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(ConfigFile));
			keyStorePath = props.getProperty(cometClientPropsKeys.COMET_CLIENT_KEYSTORE_PROP);
			keyStorePass = props.getProperty(cometClientPropsKeys.COMET_CLIENT_KEYSTORE_PASS_PROP);
			trustStorePath = props.getProperty(cometClientPropsKeys.COMET_CLIENT_TRUSTTORE_PROP);
			trustStorePass = props.getProperty(cometClientPropsKeys.COMET_CLIENT_TRUSTSTORE_PASS_PROP);
			baseURL = props.getProperty(cometClientPropsKeys.COMET_SERVICE_BASE_URI);
			accumulo_username = props.getProperty(cometClientPropsKeys.COMET_CLIENT_ACCUMULO_USERNAME);
			accumulo_password = props.getProperty(cometClientPropsKeys.COMET_CLIENT_ACCUMULO_PASSWORD);
			if(keyStorePath == null || keyStorePass == null || trustStorePath == null || trustStorePass == null
					|| baseURL == null || accumulo_username==null || accumulo_password == null) {
				log.error("Missing property in configuration File");
				throw new Exception("Missing property in configuration file");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		init();
		
	}
	private void  init() {
		KeyStore keyStore; 
		InputStream is;
		KeyManagerFactory keyManagerFactory=null;
		try {
			keyStore= KeyStore.getInstance("JKS");
			is = new FileInputStream(keyStorePath);
			keyStore.load(is, keyStorePass.toCharArray());
			is.close();
			
			keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
			keyManagerFactory.init(keyStore, keyStorePass.toCharArray());
		} catch (NoSuchAlgorithmException | CertificateException | IOException | UnrecoverableKeyException | KeyStoreException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


		SSLContext sc = null;
		HostnameVerifier allHostsValid=null;
		try {
			sc = SSLContext.getInstance("TLSv1");
			TrustManager[] trustAllCerts = { new InsecureTrustManager() };
			sc.init(keyManagerFactory.getKeyManagers(), trustAllCerts, new java.security.SecureRandom());
			allHostsValid = new InsecureHostnameVerifier();

		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Client client = ClientBuilder.newBuilder().sslContext(sc).hostnameVerifier(allHostsValid).build();
		webTarget = client.target(baseURL);
		
	}
	/*public cometClientCurl(String keystore, String ksPassword, 
			String truststore, String tsPassword, String baseUri, String username, String password) {
		keyStorePath = keystore;
		trustStorePath=truststore;
		baseURL = baseUri;
		accumulo_username=username;
		accumulo_password=password;
		keyStorePass=ksPassword;
		trustStorePass=tsPassword;
	}*/
	
	public String callMethod(String resourcePath, MultivaluedMap<String,String> parameters) {
		
		parameters.add("username", this.accumulo_username);
		parameters.add("password", this.accumulo_password);
		String response = webTarget.path("comet/createscope")
				.request()
				.post(Entity.form(parameters),String.class);
		
		System.out.println("Response: " + response);
		return response;
	}
	public static void main (String [] strs) {

	
		MultivaluedMap<String, String> map = new MultivaluedHashMap<String, String>();
		map.add("contextType", "virtualsystems");
		map.add("contextSubType","iaas");
		map.add("scopeName", "myscopename");
		map.add("visibility", "actor");
		map.add("contextID","mycontextidnxxnxxx");
		map.add("scopeValue", "myscopevalue");
	

	String configfile = "/Users/clariscastillo/Documents/projects/2016/COMET/comet-clien-curl/src/main/resources/configFile";
	cometClientCurl client = new cometClientCurl(configfile);
	client.callMethod("comet/createscope", map);


	}
}

