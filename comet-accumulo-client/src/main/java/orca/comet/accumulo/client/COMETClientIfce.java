package orca.comet.accumulo.client;
/**
 * Client API for COMET service.
 * @author claris
 *
 */
public interface COMETClientIfce {


//@TODO: Add authorization information to calls

	
	//The API Calls below affect data under the ColumnFamily: UserData 
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param hostname
	 */
	void setHostname(String SliceID, String ReservationID, String hostname);

	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param hostname
	 * @param overwrite
	 */
	void updateHostname(String SliceID, String ReservationID, String hostname, String overwrite); //default = false
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param script
	 */
	void setScript(String SliceID, String ReservationID, String script);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param users
	 */
	void setUsers(String SliceID, String ReservationID, String users);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param users
	 * @param overwrite
	 */
	void updateUsers(String SliceID, String ReservationID, String users, String overwrite); //default = false
	
	//The API Calls below affect data under the ColumnFamily:  SystemData
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param ip
	 */
	void setManagementIP(String SliceID, String ReservationID, String ip);
	/**
	 * 
	 * @param SliceID
	 * @param ReseravationID
	 * @param ip
	 * @param overwrite
	 */
	void updateManagementIP(String SliceID, String ReseravationID, String ip, String overwrite);//default=false
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param physicalHost
	 */
	void setPhysicalHost(String SliceID, String ReservationID, String physicalHost);
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param physicalHost
	 */
	void updatePhysicalHost(String SliceID, String ReservationID, String physicalHost);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param novaid
	 */
	void setNovaID(String SliceID, String ReservationID, String novaid);
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param novaid
	 */
	void updateNovaID(String SliceID, String ReservationID, String novaid);
	
	//The API Calls below affect data under the ColumnFamily:  Type
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param type
	 */
	void createNewEntry(String SliceID, String ReservationID, String type);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param type
	 */
	void setType(String SliceID, String ReservationID, String type);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param type
	 */
	 void updateType(String SliceID, String ReservationID, String type);
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param interfaceID
	 * Since there were will be many interfaces we will prefix each interface 'key" with ifce, so for example 'ifce:fe163e0063d2' By doing this we can
	 * query for all the ifaces for a given VM. Otherwise ifce enumeration is not possible.
	 */
	void setInterface(String SliceID, String ReservationID, String interfaceID, String protocol, String ipAddress, String state);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param interfaceID
	 * @param protocol
	 */
	void updateInterfaceProtocol(String SliceID, String ReservationID, String interfaceID, String protocol);		
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param interfaceID
	 * @param newState
	 */
	void updateInterfaceState(String SliceID, String ReservationID, String interfaceID, String newState);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param interfaceID
	 * @param newIPAddress
	 */
	void updateInterfaceIpAddress(String SliceID, String ReservationID, String interfaceID, String newIPAddress);
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @return list of interfaces of sliver
	 */
	String getInterfaces(String SliceID, String ReservationID);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param interfaceID
	 * @return ipaddress, null if never set, empty string if unset.
	 */
	String getInterfaceIpAddress(String SliceID, String ReservationID, String interfaceID);
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param interfaceID
	 * @return state, null if never set, empty string if unset.
	 */
	String getInterfaceState(String SliceID, String ReservationID, String interfaceID);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param interfaceID
	 * @return protocol, null if never set, empty string if unset.
	 */
	String getInterfaceProtocol(String SliceID,String ReservationID, String interfaceID);

	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @return protocol, null if never set, empty string if unset
	 */
	String getHostname(String SliceID, String ReservationID);
	
	/**
	 * Return Management IP address 
	 * @param SliceID
	 * @param ReservationID
	 * @return managementIpAddress, null if never set, empty string if unset
	 */
	String getManagementIpAddress(String SliceID, String ReservationID);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @return novaid, null if never set, empty string if unset
	 */
	String getNovaID(String SliceID, String ReservationID);
	
	/**
	 * Return the physical host hosting the VM (if vm type @see {{@link #setType(String, String, String)})
	 * @param SliceID
	 * @param ReservaitonID
	 * @return physical host, null if never set, emtpy string if unset
	 */
	String getPhysicalHost(String SliceID, String ReservationID);

	/**
	 * Return the type of the sliver
	 * @param SliceID
	 * @param ReservationID
	 * @return type of sliver, null if never set, empty string if unset
	 */
	String getType(String SliceID, String ReservationID);
	
	/**
	 * Return the script 
	 * @param SliceID
	 * @param ReservationID
	 * @return
	 */
	
	String getScript(String SliceID, String ReservationID);
	
	/*
	 * Scan all the data in COMET
	 */
	String scanData();
	
	/**
	 * Return all the interfaces for a sliceid:sliverid pair
	 * @param SliceID
	 * @param RreservationID
	 * @return
	 */
	String getIfces(String SliceID, String RreservationID);
	
	/**
	 * Return all data associated with an interfaces
	 * @param SliceID
	 * @param ReservationID
	 * @param Ifce
	 * @return
	 */
	String getIfceData(String SliceID, String ReservationID, String Ifce);
	
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param Ifce
	 * @return
	 */
	String getIfceIp(String SliceID, String ReservationID, String Ifce);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param Ifce
	 * @return
	 */
	String getIfceState(String SliceID, String ReservationID, String Ifce);
	
	/**
	 * 
	 * @param SliceID
	 * @param ReservationID
	 * @param Ifce
	 * @return
	 */
	String getIfceProtocol(String SliceID, String ReservationID, String Ifce);
	
	/**
	 * Delete *all* configuration and metadata associated with a given pair sliceID, reservationID
	 * @param SliceID
	 * @param ReservationID
	 */
	void deleteEntry(String SliceID, String ReservationID);
	
	
	String getAllData(String SliceID, String ReservationID);
	
}
