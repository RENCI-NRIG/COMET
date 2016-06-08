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
package comet.accumulo.resource;


import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.servlet.ServletContext;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import comet.accumulo.genc.COMETAdminImpl;
import comet.accumulo.genc.COMETGenCImpl;

/**
 * Root resource class for COMET Restful Web-service
 * @author claris
 * TODO Isolate Admin API? Different Web-service perhaps?
 */
@Path("/comet")
public class CometResource {

	private static final Logger log = Logger.getLogger(CometResource.class);

	String configFile;


	String contextType = null;
	String contextSubType = null;
	String contextID = null;
	String scopeName = null;
	String scopeValue = null;
	String visibility = null;
	String username = null;
	String password = null;
	String adminpass = null;
	String label = null;


	@Context
	ServletContext contextApp;

	@POST
	@Path("createscope")
	@Produces(MediaType.APPLICATION_JSON)
	public String getMessage(MultivaluedMap<String, String> formParams) throws JSONException {

		contextType = formParams.getFirst("contextType");
		contextSubType = formParams.getFirst("contextSubType");
		contextID = formParams.getFirst("contextID");
		scopeName = formParams.getFirst("scopeName");
		scopeValue = formParams.getFirst("scopeValue");
		visibility = formParams.getFirst("visibility");
		username = formParams.getFirst("username");
		password = formParams.getFirst("password");
		JSONObject jsonobject  = new JSONObject();
		if (contextType == null || contextID == null || contextSubType == null
				|| scopeValue == null || scopeName == null || username == null
				|| password == null || visibility == null) {
			return jsonobject.put("Error", "Missing parameter").toString();

		}

		configFile = contextApp.getInitParameter("configfile");

		COMETGenCImpl client = new COMETGenCImpl(configFile);


		try {
			client.init(username, password);
		} catch (AccumuloException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return jsonobject.put("error", e.getMessage()).toString();

		} catch (AccumuloSecurityException e) {
			log.error(e.getMessage());
			return jsonobject.put("error", e.getMessage()).toString();

		}

		JSONObject output = client.createScope(contextType, contextSubType, contextID, scopeName,
				scopeValue, visibility);

		return output.toString();

	}

	@POST
	@Path("readscope")
	@Produces(MediaType.APPLICATION_JSON)
	public String readScope(MultivaluedMap<String, String> formParams) {
		contextType = formParams.getFirst("contextType");
		if(contextType==null) return "contextType";
		contextSubType = formParams.getFirst("contextSubType");
		if(contextSubType==null) return "contextSubType";
		contextID = formParams.getFirst("contextID");
		if(contextID==null) return "contextID";
		scopeName = formParams.getFirst("scopeName");
		visibility = formParams.getFirst("visibility");
		if(visibility==null) return "visibility";
		username = formParams.getFirst("username");
		if(username==null) return "username";
		password = formParams.getFirst("password");
		if(password==null) return "password";	

		if (contextType == null || contextID == null || contextSubType == null
				|| scopeName == null || username == null || password == null
				|| visibility == null) {
			return "Error: Missing parameter.\n";
		}

		configFile = contextApp.getInitParameter("configfile");
		COMETGenCImpl client = new COMETGenCImpl(configFile);
		try {
			client.init(username, password);
		} catch (AccumuloException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} catch (AccumuloSecurityException e) {
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} 
		JSONObject scopeValue = client.readScope(contextType, contextSubType,
				contextID, scopeName, visibility);

		return scopeValue.toString();
	}

	@POST
	@Path("destroyscope")
	@Produces(MediaType.APPLICATION_JSON)
	public String destroyScope(MultivaluedMap<String, String> formParams) {

		contextType = formParams.getFirst("contextType");
		contextSubType = formParams.getFirst("contextSubType");
		contextID = formParams.getFirst("contextID");
		scopeName = formParams.getFirst("scopeName");
		visibility = formParams.getFirst("visibility");
		username = formParams.getFirst("username");
		password = formParams.getFirst("password");

		if (contextType == null || contextID == null || contextSubType == null
				|| scopeName == null || username == null || password == null) {
			return "Error: Missing parameter.\n";
		}

		configFile = contextApp.getInitParameter("configfile");
		COMETGenCImpl client = new COMETGenCImpl(configFile);
		try {
			client.init(username, password);
		} catch (AccumuloException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} catch (AccumuloSecurityException e) {
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} 

		JSONObject output =  client.destroyScope(contextType, contextSubType, contextID,
				scopeName, visibility);

		return output.toString();

	}

	@POST
	@Path("deleteAll")
	@Produces(MediaType.APPLICATION_JSON)
	public String deleteAllContextType(MultivaluedMap<String,String> formParams) {
		contextType = formParams.getFirst("contextType");
		visibility = formParams.getFirst("visibility");
		username = formParams.getFirst("username");
		password = formParams.getFirst("password");
		if(contextType == null || username == null || password == null) {
			return "Error: Missing parameter.\n";
		}

		configFile = contextApp.getInitParameter("configfile");
		COMETGenCImpl client = new COMETGenCImpl(configFile);
		try {
			client.init(username, password);
		} catch (AccumuloException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} catch (AccumuloSecurityException e) {
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} 
		return client.deleteAllRowsInTable(contextType);
	}
	@POST
	@Path("modifyscope")
	@Produces(MediaType.APPLICATION_JSON)
	public String modifyScope(MultivaluedMap<String, String> formParams) {
		contextType = formParams.getFirst("contextType");
		contextSubType = formParams.getFirst("contextSubType");
		contextID = formParams.getFirst("contextID");
		scopeName = formParams.getFirst("scopeName");
		scopeValue = formParams.getFirst("scopeValue");
		visibility = formParams.getFirst("visibility");
		username = formParams.getFirst("username");
		password = formParams.getFirst("password");

		if (contextType == null || contextID == null || contextSubType == null
				|| scopeValue == null || scopeName == null || username == null
				|| password == null || visibility == null) {
			return "Error: Missing parameter.\n";
		}

		configFile = contextApp.getInitParameter("configfile");
		COMETGenCImpl client = new COMETGenCImpl(configFile);
		try {
			client.init(username, password);
		} catch (AccumuloException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} catch (AccumuloSecurityException e) {
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} 

		JSONObject output= client.modifyScope(contextType, contextSubType, contextID,
				scopeName, scopeValue, visibility);

		return output.toString();


	}


	/**
	 * Admin API POJO methods are below. We should consider to isolate them.
	 */

	@POST
	@Path("adduser")
	@Produces(MediaType.APPLICATION_JSON)
	public String addUser(MultivaluedMap<String, String> formParams) {
		JSONObject output;
		username = formParams.getFirst("username"); //useradded
		password = formParams.getFirst("password"); //password of new user
		adminpass = formParams.getFirst("adminpass"); //accumulo root password

		List<ByteBuffer> labelsBB = new  ArrayList<ByteBuffer>();
		List<String> labels = formParams.get("labels");

		if (username == null || password == null || adminpass == null) {
			return "Error: Missing parameter.\n";
		}
		for (String string : labels) {
			try {
				byte[] b = string.getBytes("UTF-8");
				ByteBuffer bb = ByteBuffer.wrap(b);
				labelsBB.add(bb);
			} catch (UnsupportedEncodingException e) {
				log.error("Unsupported Encoding Exception: " + e.getMessage());

			}

		}

		configFile = contextApp.getInitParameter("configfile");
		COMETGenCImpl client = new COMETGenCImpl(configFile);
		try {
			client.init("root", adminpass);
			COMETAdminImpl adminImpl = new COMETAdminImpl("root", adminpass, client.getConnector(), client.getClientConfig());
			output = adminImpl.addUser(username, password, labelsBB);

		} catch (AccumuloException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} catch (AccumuloSecurityException e) {
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} 
		return output.toString();

	}

	/*@POST
	@Path("deleteuser")
	@Produces(MediaType.APPLICATION_JSON)
	public String deleteUser(MultivaluedMap<String, String> formParams) {
		username = formParams.getFirst("username");
		password = formParams.getFirst("password");
		JSONObject output= new JSONObject();

		if (username == null || password == null) {
			return "Error: Missing parameter.\n";
		}


		return output.toString();
	}
	 */

	@POST
	@Path("addauthlabel")
	@Produces(MediaType.APPLICATION_JSON)
	public String addAuthLabel(MultivaluedMap<String, String> formParams) {
		username = formParams.getFirst("username");
		adminpass = formParams.getFirst("adminpass");
		
		List<ByteBuffer> labelsBB = new  ArrayList<ByteBuffer>();
		List<String> labels = formParams.get("labels");

		JSONObject output = new JSONObject();
		if (username == null || adminpass == null || labels == null) {
			return "Error: Missing parameter.\n";
		}

		for (String string : labels) {
			try {
				byte[] b = string.getBytes("UTF-8");
				ByteBuffer bb = ByteBuffer.wrap(b);
				labelsBB.add(bb);
			} catch (UnsupportedEncodingException e) {
				log.error("Unsupported Encoding Exception: " + e.getMessage());

			}

		}
		configFile = contextApp.getInitParameter("configfile");
		COMETGenCImpl client = new COMETGenCImpl(configFile);
		try {
			client.init("root", adminpass);
			COMETAdminImpl adminImpl = new COMETAdminImpl("root", adminpass, client.getConnector(), client.getClientConfig());
			output = adminImpl.addAuthorizations(username, labelsBB);

		} catch (AccumuloException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} catch (AccumuloSecurityException e) {
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} 

		return output.toString();
	}

	@POST
	@Path("removeauthlabel")
	@Produces(MediaType.APPLICATION_JSON)
	public String removeAuthLabel(MultivaluedMap<String, String> formParams) {
		username = formParams.getFirst("username");
		adminpass = formParams.getFirst("adminpass");
		label = formParams.getFirst("label");
		
		JSONObject output = new JSONObject();
		if (username == null || adminpass == null || label==null) {
			return "Error: Missing parameter.\n";
		}

		try {
			byte[] b = label.getBytes("UTF-8");
			ByteBuffer bb = ByteBuffer.wrap(b);
			configFile = contextApp.getInitParameter("configfile");
			COMETGenCImpl client = new COMETGenCImpl(configFile);
			try {
				client.init("root", adminpass);
				COMETAdminImpl adminImpl = new COMETAdminImpl("root", adminpass, client.getConnector(), client.getClientConfig());
				output = adminImpl.removeAuthorization(username, bb);

			} catch (AccumuloException e) {
				e.printStackTrace();
				log.error(e.getMessage());
				return (e.getMessage() + '\n');
			} catch (AccumuloSecurityException e) {
				log.error(e.getMessage());
				return (e.getMessage() + '\n');
			} 
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}




		return output.toString();
	}

	@POST
	@Path("setauthlabel")
	@Produces(MediaType.APPLICATION_JSON)
	public String setAuthLabel(MultivaluedMap<String, String> formParams) {
		username = formParams.getFirst("username");
		adminpass = formParams.getFirst("adminpass");
		List<String> labels = formParams.get("labels");
		List<ByteBuffer> labelsBB = new ArrayList<ByteBuffer>();
		JSONObject output = new JSONObject();
	
		if (username == null || adminpass == null || label==null) {
			return "Error: Missing parameter.\n";
		}
		
		for (String string : labels) {
			try {
				byte[] b = string.getBytes("UTF-8");
				ByteBuffer bb = ByteBuffer.wrap(b);
				labelsBB.add(bb);
			} catch (UnsupportedEncodingException e) {
				log.error("Unsupported Encoding Exception: " + e.getMessage());

			}

		}
		
		
		configFile = contextApp.getInitParameter("configfile");
		COMETGenCImpl client = new COMETGenCImpl(configFile);
		try {
			client.init("root", adminpass);
			COMETAdminImpl adminImpl = new COMETAdminImpl("root", adminpass, client.getConnector(), client.getClientConfig());
			output = adminImpl.setAuthorizations(username, labelsBB);

		} catch (AccumuloException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} catch (AccumuloSecurityException e) {
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} 

		return output.toString();
	}

	@POST
	@Path("listauthlabel")
	@Produces(MediaType.APPLICATION_JSON)
	public String enumAuthLabels(MultivaluedMap<String, String> formParams) {
		username = formParams.getFirst("username");
		adminpass = formParams.getFirst("adminpass");
		JSONObject output = new JSONObject();
		
		if (username == null || adminpass == null) {
			return "Error: Missing parameter.\n";
		}
		
		configFile = contextApp.getInitParameter("configfile");
		COMETGenCImpl client = new COMETGenCImpl(configFile);
		try {
			client.init("root", adminpass);
			COMETAdminImpl adminImpl = new COMETAdminImpl("root", adminpass, client.getConnector(), client.getClientConfig());
			output = adminImpl.getVisibilityLabelByteBuffer(username);

		} catch (AccumuloException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} catch (AccumuloSecurityException e) {
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		} 

		return output.toString();
	}


	@POST
	@Path("listusers")
	@Produces(MediaType.APPLICATION_JSON)
	public String enumUsers(MultivaluedMap<String, String> formParams) {
		
		adminpass = formParams.getFirst("adminpass");
		JSONObject output= new JSONObject();
		
		if (adminpass == null) {
			return "Error: Missing parameter.\n";
		}
		
		
		return output.toString();

	}
}