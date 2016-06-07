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


/**
 * Interface of General Client Interface for COMET
 * @author claris
 * TODO 
 * TODO
 * TODO
 * 
 */


package comet.accumulo.genc;

import org.codehaus.jettison.json.JSONObject;

public interface COMETGenCIfce {

	/**
	 * Create new scope. It will overwrite if scope already exists.
	 * @param contextType
	 * @param contextSubType
	 * @param contextID
	 * @param scopeName
	 * @param scopeValue
	 * @param visibility
	 * @return 
	 */
	JSONObject createScope(String contextType, String contextSubType,String contextID, String scopeName, String scopeValue, String visibility);

	/**
	 * Destroy existing scope. 
	 * @param contextType
	 * @param contextSubType
	 * @param contextID
	 * @param scopeName
	 * @param visibility
	 * @return contextID if successful or error message if not
	 */
	JSONObject destroyScope(String contextType, String contextSubType, String contextID, String scopeName, String visibility);

	/**
	 * Read scope.
	 * @param contextType
	 * @param contextSubType
	 * @param contextID
	 * @param scopeName
	 * @param authorization labels
	 * @return
	 */
	JSONObject readScope(String contextType, String contextSubType, String contextID, String scopeName, String visibility);
	/**
	 * 
	 * @param contextType
	 * @param contextSubType
	 * @param contextID
	 * @param scopeName
	 * @param newScopeValue
	 * @param visibility
	 * @return
	 */
	JSONObject modifyScope(String contextType, String contextSubType, String contextID, String scopeName, String newScopeValue, String visibility);



	/**
	 * Enumerate "visible" scopes for a given contextType and contextSubType provided with a list of  authorization labels
	 * @param contextType
	 * @param contextSubType
	 * @param contextID
	 * @param visibility
	 * @return list of scopes names
	 */
	JSONObject enumerateScopes(String username, String contextType, String contextSubType, String contextID, String visibility);

}

