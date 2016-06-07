package orca.comet.accumulo.client;

import java.io.IOException;
import java.util.Properties;

import jline.internal.Log;
import orca.comet.accumulo.client.utils.COMETClientUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CometCli {

	public static void main(String[] args) {
		Options options = new Options();

		Character sep = new Character(' ');

		Option getData = Option.builder("getData").hasArg(true)
				.desc("Return all the data associated with a sliceID, sliverID pair.)")
				.numberOfArgs(2)
				.argName("sliceID> <sliverID")
				.valueSeparator(sep)
				.build();
		
		options.addOption(getData);
						
		Option fullScan = Option.builder("fullScan").hasArg(false)
				.desc("Return all the existing entries in COMET service")
				.build();

		options.addOption(fullScan);
		Option setHostname = Option.builder("setHostname").hasArg(true)
				.numberOfArgs(3).argName("sliceID> <sliverID> <hostname")
				.desc("Set the hostname of the sliver virtual machine")
				.valueSeparator(sep).build();
		options.addOption(setHostname);
		Option setMgmtIP = Option
				.builder("setManagementIp")
				.argName("sliceID> <sliverID> <ipaddress")
				.hasArg(true)
				.numberOfArgs(3)

				.desc("Set the management ipaddress of the sliver virtual machine")
				.valueSeparator(sep).build();
		options.addOption(setMgmtIP);
		Option setScript = Option.builder("setScript").hasArg(true)
				.numberOfArgs(3).argName("sliceID> <sliverID> <script")
				.desc("Set a postboot script of the sliver virtual machine")
				.valueSeparator(sep).build();
		options.addOption(setMgmtIP);
		Option setPhysicalHost = Option.builder("setPhysicalHost").hasArg(true)
				.numberOfArgs(3)
				.desc("Set physical host of the sliver virtual machine")
				.argName("sliceID> <sliverID> <physicalhost")
				.valueSeparator(sep).build();
		options.addOption(setPhysicalHost);
		Option setNovaId = Option.builder("setNovaId").hasArg(true)
				.numberOfArgs(3).argName("sliceID> <sliverID> <novaID")
				.desc("Set Nova ID  of the sliver virtual machine")
				.valueSeparator(sep).build();
		options.addOption(setNovaId);
		Option setType = Option.builder("setType").hasArg(true).numberOfArgs(3)
				.desc("Set type  of the sliver")
				.argName("sliceID> <sliverID> <sliver type")
				.valueSeparator(sep).build();
		options.addOption(setType);
		Option setInterfaceProtocol = Option.builder("setInterfaceProtocol")
				.hasArg(true).numberOfArgs(4)
				.argName("sliceID> <sliverID> <ifce(mac)> <protocol")
				.desc("Set protocol of interface.").valueSeparator(sep).build();
		options.addOption(setInterfaceProtocol);
		Option setInterfaceState = Option.builder("setInterfaceState")
				.hasArg(true).numberOfArgs(4)
				.argName("sliceID> <sliverID> <ifce<mac> <state")
				.desc("Set state of interface.").valueSeparator(sep).build();
		options.addOption(setInterfaceState);
		Option setInterfaceIp = Option.builder("setInterfaceIp").hasArg(true)
				.numberOfArgs(4)
				.argName("sliceID> <sliverID> <ifce(mac> <ipaddress")
				.desc("Set ip address of interface.").valueSeparator(sep)
				.build();
		options.addOption(setInterfaceIp);
		Option setInterface = Option
				.builder("setInterface")
				.hasArg(true)
				.numberOfArgs(6)

				.argName(
						"sliceID> <sliverID> <ifce(mac)> <protocol> <ipaddress> <state")
				.desc("Set a new interface for a sliver virtual machine.")
				.valueSeparator(sep).build();
		options.addOption(setInterface);
		Option help = Option.builder("help").hasArg(false)
				.desc("Describe help for COMET Client Shell.").build();
		options.addOption(help);

		Option createEntry = Option.builder("createEntry").hasArg(true)
				.numberOfArgs(3).argName("sliceID> <sliverID> <sliver type")
				.desc("Create a new entry for a sliver in COMET.")
				.valueSeparator(sep).build();
		options.addOption(createEntry);
		
		Option deleteEntry = Option.builder("deleteEntry").hasArg(true)
				.numberOfArgs(2).argName("sliceID> <sliverID")
				.desc("Delete entry from COMET.")
				.valueSeparator(sep).build();
		options.addOption(deleteEntry);

		/** Getters **/
		Option getHostname = Option.builder("getHostname")
				.desc("Get hostname for a given sliver.").hasArg(true)
				.argName("sliceID> <sliverID").numberOfArgs(2).build();
		options.addOption(getHostname);

		Option getType = Option.builder("getType")
				.desc("Get type for a given sliver.").hasArg(true)
				.argName("sliceID> <sliverID").numberOfArgs(2).build();
		options.addOption(getType);
		Option getMgmtIp = Option.builder("getMgmtIp")
				.desc("Get management ip for a given sliver.").hasArg(true)
				.argName("sliceID> <sliverID").numberOfArgs(2).build();
		options.addOption(getMgmtIp);

		Option getIfcesNumber = Option.builder("getNumIfces")
				.desc("Get number of interfaces  for a given sliver.")
				.hasArg(true).argName("sliceID> <sliverID").numberOfArgs(2)
				.build();
		options.addOption(getIfcesNumber);

		Option getScript = Option.builder("getScript")
				.desc("Get script for a given sliver.").hasArg(true)
				.argName("sliceID> <sliverID").numberOfArgs(2).build();
		options.addOption(getScript);

		Option getPhysicalHost = Option.builder("getPhysicalHost")
				.desc("Get physical host for a given sliver.").hasArg(true)
				.argName("sliceID> <sliverID").numberOfArgs(2).build();
		options.addOption(getPhysicalHost);

		Option getIfces = Option.builder("getIfces")
				.desc("Get interfaces for a given sliver.").hasArg(true)
				.argName("sliceID> <sliverID").numberOfArgs(2).build();
		options.addOption(getIfces);

		Option getIfceProtocol = Option.builder("getIfceProtocol")
				.desc("Get protocol of interface.").hasArg(true)
				.argName("sliceID> <sliverID> <ifce(mac").numberOfArgs(3)
				.build();
		options.addOption(getIfceProtocol);

		Option getIfceIpAddress = Option.builder("getIfceIpAddress")
				.desc("Get ip address of interface.").hasArg(true)
				.argName("sliceID> <sliverID> <ifce(mac").numberOfArgs(3)
				.build();
		options.addOption(getIfceIpAddress);

		Option getIfceState = Option.builder("getIfceState")
				.desc("Get state of interface.").hasArg(true)
				.argName("sliceID> <sliverID> <ifce(mac").numberOfArgs(3)
				.build();
		options.addOption(getIfceState);

		Option getIfceData = Option.builder("getIfceData")
				.desc("Get all data associated with an interface.")
				.hasArg(true).argName("sliceID> <sliverID> <ifce(mac)")
				.numberOfArgs(3).build();
		options.addOption(getIfceData);

		
		Option configFile = Option
				.builder("configFile")
				.desc("Pass configuration information for Accumulo and Zookeeper.")
				.hasArg(true).required(true)
				.argName("Complete path of configuration file").numberOfArgs(1)
				.build();

		options.addOption(configFile);

		CommandLineParser parser = new DefaultParser();
		Properties props = new Properties();
		/*
		 * try { // props = COMETClientUtils.getClientConfigProps(
		 * "/Users/claris/git/COMET/comet-accumulo-client/src/main/resources/config.properties"
		 * ); props = COMETClientUtils.getClientConfigProps(
		 * "/Users/claris/git/COMET/comet-accumulo-client/src/main/resources/config.properties"
		 * ); } catch (IOException e) { e.printStackTrace(); }
		 */

		String header = "COMET shell command client\n\n";
		String footer = "\nPlease report issues to the person sitting in the corner office.";

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("COMETCLI", header, options, footer, true);

		COMETClientImpl clientImpl = null;
		// = new COMETClientImpl(props);
		try {
			CommandLine cmd = parser.parse(options, args, true);

			Option[] optionArray = cmd.getOptions();

			for (Option option : optionArray) {
				String[] values = option.getValues();

				if (option.equals(configFile)) {
					try {
						props = COMETClientUtils
								.getClientConfigProps(values[0]);
						clientImpl = new COMETClientImpl(props);
						;
					} catch (IOException e) {
						Log.error("IOException on file " + values[0] + "."
								+ e.getMessage());
					}
				}
				
				

				if (option.equals(setHostname)) {
					clientImpl.setHostname(values[0], values[1], values[2]);
				}
				if (option.equals(setMgmtIP)) {

					clientImpl.setHostname(values[0], values[1], values[2]);
				}
				if (option.equals(setScript)) {

					clientImpl.setScript(values[0], values[1], values[2]);
				}

				if (option.equals(setPhysicalHost)) {

					clientImpl.setPhysicalHost(values[0], values[1], values[2]);
				}
				if (option.equals(setNovaId)) {

					clientImpl.setNovaID(values[0], values[1], values[2]);
				}

				if (option.equals(setType)) {

					clientImpl.setType(values[0], values[1], values[2]);
				}

				if (option.equals(setInterfaceProtocol)) {
					clientImpl.updateInterfaceProtocol(values[0], values[1],
							values[2], values[3]);

				}
				if (option.equals(setInterfaceState)) {
					clientImpl.updateInterfaceState(values[0], values[1],
							values[2], values[3]);

				}

				if (option.equals(fullScan)) {
					clientImpl.scanData();
				}

				if (option.equals(setInterfaceIp)) {
					clientImpl.updateInterfaceIpAddress(values[0], values[1],
							values[2], values[3]);
				}

				if (option.equals(setInterface)) {
					clientImpl.setInterface(values[0], values[1], values[2],
							values[3], values[4], values[5]);
				}

				if (option.equals(createEntry)) {
					clientImpl.createNewEntry(values[0], values[1], values[2]);
				}
				
				if(option.equals(deleteEntry)) {
					clientImpl.deleteEntry(values[0], values[1]);
				}

				if (option.equals(getHostname)) {
					System.out.println(" Value: "
							+ clientImpl.getHostname(values[0], values[1]));

				}

				if (option.equals(getMgmtIp)) {
					System.out.println(" Value: "
							+ clientImpl.getManagementIpAddress(values[0],
									values[1]));
				}

				if (option.equals(getPhysicalHost)) {
					System.out.println(" Value: "
							+ clientImpl.getPhysicalHost(values[0], values[1]));
				}

				if (option.equals(getScript)) {
					System.out.println(" Value: "
							+ clientImpl.getScript(values[0], values[1]));
				}

				if (option.equals(getType)) {
					System.out.println(" Value: "
							+ clientImpl.getType(values[0], values[1]));
				}

				if (option.equals(getIfces)) {
					clientImpl.getIfces(values[0], values[1]);
				}

				if (option.equals(getIfceData)) {
					clientImpl.getIfceData(values[0], values[1], values[2]);
				}

				if (option.equals(getIfceIpAddress)) {
					clientImpl.getIfceIp(values[0], values[1], values[2]);

				}

				if (option.equals(getIfceProtocol)) {
					clientImpl.getIfceProtocol(values[0], values[1], values[2]);

				}
				if (option.equals(getIfceState)) {
					clientImpl.getIfceState(values[0], values[1], values[2]);
				}

				if(option.equals(getData)) {
					clientImpl.getAllData(values[0], values[1]);
				}
				
				if (option.equals(help)) {
					formatter.printHelp("COMETClientShellImpl", options);
				}
			}

		} catch (ParseException e) {
			Log.error("Parsing failed. Reason: " + e.getMessage());

		}

	}

}
