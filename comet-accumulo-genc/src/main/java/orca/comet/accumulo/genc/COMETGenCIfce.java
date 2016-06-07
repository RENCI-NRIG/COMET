package orca.comet.accumulo.genc;

public interface COMETGenCIfce {

	String createScope(String contextType, String contextSubType,String contextID, String scopeName, String scopeValue, String visibility);
	
	String destroyScope(String contextType, String contextSubType, String contextID, String scopeName, String visibility);
	
	String readScope(String contextType, String contextSubType, String contextID, String scopeName, String visibility);
	
	String modifyScope(String contextType, String contextSubType, String contextID, String scopeName, String newScopeValue, String visibility);
	
	}
