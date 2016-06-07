package orca.comet.accumulo.client.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.log4j.Logger;

public class COMETClientUtils {

	private static final Logger log = Logger.getLogger(COMETClientUtils.class);

	public static Properties getClientConfigProps(String filePathString)
			throws IOException {

		Path path = Paths.get(filePathString);
		Properties props = new Properties();

		if (Files.notExists(path)) {
			log.error("Client configuration file  " + filePathString
					+ " not found.");
			throw new FileNotFoundException(filePathString);
		}

		props.load(new FileInputStream(filePathString));

	//	if (log.getLevel().isGreaterOrEqual(Level.DEBUG)) {
			PrintWriter writer = new PrintWriter(System.out);
			props.list(writer);
			writer.flush();
	//	}

		if(!checkProperties(props)) {
			try {
				throw new Exception("Missing property");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return props;
	}

	private static boolean checkProperties(Properties props){
		
		
		return true;
	}
}
