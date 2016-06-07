package comet.accumulo.model;

import java.io.Serializable;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ScopeEntry implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String contextID;
	String contextType;
	String contextSubType;
	String scopeName;
	String scopeValue;
	String visibility=null;
	
	//Must have a no-argument constructor
	public ScopeEntry() {
	}
	
	
	public String getContextID() {
		return this.contextID;
	}
	
	public String getContextType() {
		return this.contextType;
	}
	
	public String getContextSubType() {
		return this.contextSubType;
	}
	
	public String getScopeName() {
		return this.scopeName;
	}
	
	public String getScopeValue() {
		return this.scopeValue;
	}
	
	public String getVisibility() {
		return this.visibility;
	}
	
	public ScopeEntry(String contextID, String contextType, String contextSubType, 
			String scopeName, String scopeValue) {
		
		this.contextID=contextID;
		this.contextType=contextType;
		this.contextSubType=contextSubType;
		this.scopeName=scopeName;
		this.scopeValue = scopeValue;
	}
	
	public void setVisibility(String visibility) {
		this.visibility=visibility;
	}
	
	@Override
	public String toString() {
		return new StringBuffer(" contextID: ").append(this.contextID)
				.append(" contextType: ").append(this.contextType)
				.append(" contextSubType: ").append(this.contextSubType)
				.append(" scopeName: ").append(this.scopeName)
				.append(" scopeValue: ").append(this.scopeValue).toString();
		
	}
	
	public JSONObject toJSON() throws JSONException {
		
		JSONObject obj = new JSONObject();
		obj.put("contextID", this.contextID);
		obj.put("contextType", this.contextType);
		obj.put("contextSubType",this.contextSubType);
		obj.put("scopeName", this.scopeName);
		obj.put("scopeValue", this.scopeValue);
		
		return obj;
		
	}
}
