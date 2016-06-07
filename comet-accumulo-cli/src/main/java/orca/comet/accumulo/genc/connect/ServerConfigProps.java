package orca.comet.accumulo.genc.connect;


public class ServerConfigProps {

	/* Currently the scope for these prperties is global. 
	 * We may need to reduce the scope at the levle of tables in the future to adapt to table usage
	 * 
	 */
	public static String COMET_CLIENT_ACCUMULO_MEMBUFF = "comet.client.accumulo.membuff";
	public static String COMET_CLIENT_ACCUMULO_NUMTHREADS = "comet.client.accumulo.numthreads";
	
}
