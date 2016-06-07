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
 * Accumulo-sepcific configuration properties to be used by Accumulo client code embedded in the COMET webapp to interact with Accumulo
 * Properties such as location of client keystore, trustore, number of threads, root password, table  names, etc.
 * These properties are to be feed into webapp via ConfigurationFile property file
 * @author claris
 *TODO Better separation of client and service configuration properties
 *TODO Include a property for provider(user) FamQualifier (Data schema) and associated list of authorization labels
 */
public class ClientConfigProps {
	
	/*CLIENT CONFIGURATION INFORMATION*/
	public static String COMET_CLIENT_KEYSTORE_PROP = "comet.client.keystore";
	public static String COMET_CLIENT_TRUSTTORE_PROP = "comet.client.truststore";
	public static String COMET_CLIENT_KEYSTORE_PASS_PROP =  "comet.client.keystore.pass";
	public static String COMET_CLIENT_TRUSTSTORE_PASS_PROP = "comet.client.truststore.pass";
	public static String COMET_CLIENT_ACCUMULO_INSTANCE_PROP = "comet.client.accumulo.instance";
	public static String COMET_CLIENT_ZOOKEEPERS_HOSTS_PROP = "comet.client.zookeeperse.hosts";
	public static String COMET_CLIENT_ACCUMULO_MAINTABLE_PROP = "comet.client.table.main";
	public static String COMET_CLIENT_ACCUMULO_USER_DEFAULT = "comet.client.accumulo.user.default";
	public static String COMET_CLIENT_ACCUMULO_USER_DEFAULT_PASS = "comet.client.accumulo.user.default.pass";
	public static String COMET_CLIENT_ACCUMULO_IFCETABLE_PROP = "comet.client.table.ifce";
	public static String COMET_CLIENT_ACCUMULO_MEMBUFF = "comet.client.accumulo.membuff";
	public static String COMET_CLIENT_ACCUMULO_NUMTHREADS = "comet.client.accumulo.numthreads";
	public static String COMET_CLIENT_ACCUMULO_KEYSTORE_TYPE="coment.client.keystore.type";
	public static String COMET_CLIENT_ACCUMULO_TRUSTSTORE_TYPE="comet.client.truststore.type";
	
	public static String COMET_CLIENT_ACCUMULO_LABEL_ACTOR = "comet.service.auth.label.actor";
	public static String COMET_CLIENT_ACCUMULO_LABEL_USER = "comet.service.auth.label.user";
	
	/*SERVICE CONFIGURATION INFORMATION*/
	public static String COMET_SERVICE_ACCUMULO_TABLE_SLICES = "comet.service.accumulo.table.slices";
	public static String COMET_SERVICE_ACCUMULO_TABLE_RESERVATIONS =  "comet.service.accumulo.table.reservations";
	public static String COMET_SERVICE_ACCUMULO_TABLE_PRINCIPALS = "comet.service.accumulo.table.principals";
	public static String COMET_SERVICE_ACCUMULO_AUTH_ROOTPASSWORD = "comet.service.auth.root.password";
	public static String COMET_SERVICE_ACCUMULO_AUTH_ROOT= "comet.service.auth.root";
	public static String COMET_SERVICE_ACCUMULO_AUTH_LABEL_USER="comet.service.auth.label.user";
	public static String COMET_SERVICE_ACCUMULO_AUTH_LABEL_ACTOR="comet.service.auth.label.actor";
	public static String COMET_SERVICE_ACCUMULO_DATASCHEMA_IAAS="comet.service.dataschema.iaas";
	public static String COMET_SERVICE_ACCUMULO_DATASCHEMA_USER="comet.service.dataschema.user";
	

	
	
}
