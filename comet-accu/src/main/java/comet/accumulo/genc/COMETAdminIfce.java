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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * Interface to run  administrative operations against COMET
 */

/**Maintain an updated to-do list
TODO Add all the operations supported by {@link org.apache.accumulo.core.client.admin.TableOperations}
TODO Add additional operations supported by {@link org.apache.accumulo.core.client.impl.SecurityOperationsImpl}. Avoid adding system grant permissions unless needed. We should use the shell tool fo these.
TODO 
**/

public interface COMETAdminIfce {

	/**
	 * Adduser to COMET service
	 * @param username
	 * @param password
	 * @param authorizations List of labels/tokens for this user
	 * @return username if succesfull, error message if not.
	 */
	public String addUser(String username, String password, List<ByteBuffer> authorizations);
	/**
	 * Set authorization labels for user. This method overwrite existing authorization labels
	 * @param username
	 * @param authoriations
	 * @return username if succesful, error message if not
	 */
	public String setAuthorizations(String username, List<ByteBuffer> authoriations);
	/**
	 * Enumerate authorization labels associated with this user
	 * @param username
	 * @return List of authorization labels (ByteBuffer)
	 */
	public List<ByteBuffer> getVisibilityLabelByteBuffer(String username);
	/**
	 * Remove user from COMET service. This method does not modify data created by this user and possible inaccessible in the future. Additional administrative operations needed.
	 * @param username
	 * @return
	 */
	public String removeUser(String username);
	/**
	 * Enumerate existing users in COMET. This only enumerate their usernames
	 * @return
	 */
	public Set<String> enumerateUsers();
	/**
	 * Remove one authorization label from an existing user
	 * @param username
	 * @param authorizatoin label
	 * @return username if succesful, 
	 */
	public String removeAuthorization(String username, ByteBuffer label);
	/**
	 * Add authorization label(s) to username
	 * @param username
	 * @param authorizations
	 * @return username if successful, error message if not.
	 */
	public String addAuthorizations(String username, List<ByteBuffer> authorizations);
	/**
	 * Grant table permission to user. After created a user has
	 * @param tableName
	 * @param username
	 * @param permission
	 * @return String tablename if succcessful, error message if not
	 */
	public String grantTablePermission(String tableName, String username, String permission);
	/**
	 * Revoke table permission 
	 * @param tableName
	 * @param username
	 * @param permission
	 * @return
	 */
	public String revokeTablePermission(String tableName, String username, String permission);

	
}
