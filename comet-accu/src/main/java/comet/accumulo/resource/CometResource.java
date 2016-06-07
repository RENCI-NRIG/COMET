package comet.accumulo.resource;

import javax.ws.rs.GET;
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

import comet.accumulo.genc.COMETGenCImpl;

@Path("/comet")
public class CometResource {

	private static final Logger log = Logger.getLogger(CometResource.class);

	String configFile;

	// String configFile = "/opt/tomcat/configFile";

	String contextType = null;
	String contextSubType = null;
	String contextID = null;
	String scopeName = null;
	String scopeValue = null;
	String visibility = null;
	String username = null;
	String password = null;

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
			
		} catch (TableExistsException e) {
			e.printStackTrace();
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
		} catch (TableExistsException e) {
			e.printStackTrace();
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
		} catch (TableExistsException e) {
			e.printStackTrace();
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
		} catch (TableExistsException e) {
			e.printStackTrace();
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
		} catch (TableExistsException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return (e.getMessage() + '\n');
		}
		
		JSONObject output= client.modifyScope(contextType, contextSubType, contextID,
				scopeName, scopeValue, visibility);
		
		return output.toString();
		

	}

}