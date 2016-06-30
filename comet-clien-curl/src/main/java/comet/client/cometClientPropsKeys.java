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

public class cometClientPropsKeys {
	
	/*SSL CONTEXT INFORMATION*/
	public static String COMET_CLIENT_KEYSTORE_PROP = "comet.client.keystore";
	public static String COMET_CLIENT_TRUSTTORE_PROP = "comet.client.truststore";
	public static String COMET_CLIENT_KEYSTORE_PASS_PROP =  "comet.client.keystore.pass";
	public static String COMET_CLIENT_TRUSTSTORE_PASS_PROP = "comet.client.truststore.pass";
	
	/*RESTFUL API BASE URI*/
	public static String COMET_SERVICE_BASE_URI = "comet.service.uri.base";
	
	/*RESOURCE URL PATHS*/
	public static String COMET_SERVICE_RSRCE_CREATE = "comet.service.rsrce.create";
	public static String COMET_SERVICE_RSRCE_DESTROY = "comet.service.rsrce.destroy";
	public static String COMET_SERVICE_RSRCE_UPDATE= "comet.service.rsrce.update";
	public static String COMET_SERVICE_RSRCE_READ = "comet.service.rsrce.read";
	public static String COMET_SERVICE_RSRCE_ENUM = "comet.service.rsrce.enum";

	/*COMET AUTHENTICATION*/
	public static String COMET_CLIENT_ACCUMULO_USERNAME = "comet.client.accumulo.username";
	public static String COMET_CLIENT_ACCUMULO_PASSWORD = "comet.client.accumulo.password";

			
}
