package com.alvazan.ssql.cmdline;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.codec.binary.Hex;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;

public class PlayOrm {

	private static final Logger log = LoggerFactory.getLogger(PlayOrm.class);
	private NoSqlEntityManagerFactory factory;
	private CmdHelp help = new CmdHelp();
	private CmdSelect select = new CmdSelect();
	
	public PlayOrm(NoSqlEntityManagerFactory factory) {
		this.factory = factory;
	}

	public static void main(String[] args) {
		
		byte[] data = StandardConverters.convertToBytes("ejktest001:time");
		char[] str = Hex.encodeHex(data);
		String s = new String(str);
		log.info("s="+s);
		
		PlayOptions bean = new PlayOptions();
	    CmdLineParser parser = new CmdLineParser(bean);
		try {
		    parser.parseArgument(args);
		} catch( CmdLineException e ) {
		    printErr(e.getMessage());
		    printUsage(parser);
		    System.exit(-1);
		}
		
		DbTypeEnum storeType = DbTypeEnum.lookup(bean.getType());
		if(storeType == null) {
			printErr("type must be inmemory or cassandra");
			printUsage(parser);
		    System.exit(-2);
		} else if(storeType == DbTypeEnum.CASSANDRA) {
			if(bean.getKeyspace() == null || bean.getSeeds() == null) {
				printErr("-k and -s must be specified with -t cassandra");
				printUsage(parser);
				System.exit(-3);
			}
		}

		if(!bean.isVerbose()) {
			ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
			root.setLevel(Level.WARN);
		}
			
		Map<String, Object> properties = new HashMap<String, Object>();
		Bootstrap.createAndAddBestCassandraConfiguration(properties , "", bean.getKeyspace(), bean.getSeeds());
		properties.put(Bootstrap.AUTO_CREATE_KEY, "create");
		
		NoSqlEntityManagerFactory factory = Bootstrap.create(storeType, properties);
		new PlayOrm(factory).start();
	}

	private static void printErr(String msg) {
		System.err.println(msg);
	}

	private static void printUsage(CmdLineParser parser) {
		System.err.println("playcli ([option] argument)...");
		parser.printUsage(System.err);
	}

	private void start() {
		System.out.println("Welcome to PlayOrm Command Line");
		System.out.println("Type 'help;' for help");
		System.out.println("Type 'exit;' to exit");
		
		System.out.print("playorm >> ");
        Scanner sc = new Scanner(System.in);
        String allLines = "";
        while(sc.hasNext()){
        	allLines = processAnotherLine(sc, allLines);
        }
	}

	private String processAnotherLine(Scanner sc, String allLines2) {
		try {
			String allLines = allLines2;
			String line = sc.nextLine();
			String newLine = line.trim();
			allLines += newLine;
			if(newLine.endsWith(";")) {
				process(allLines);
				allLines = "";
				System.out.print("playorm >> ");
			} else {
				System.out.print("...     ");
			}
			return allLines;
		} catch(Exception e) {
			log.warn("Exception occurred", e);
			println("Sorry, we ran into a bug, recovering now so you can continue using command line");
			System.out.print("playorm >> ");
			return "";
		}
	}

	private void process(String newLine) {
		log.debug("processing line="+newLine);
		String[] commands = newLine.split(";");
		
		for(String cmd : commands) {
			try {
				String justCmd = cmd.trim();
				processCommand(justCmd);
			} catch (InvalidCommand e) {
				log.info("invalid command", e);
				println("Invalid command: '"+cmd+"'.  Skipping it");
				if(e.getMessage() != null)
					println(e.getMessage());
			}
		}
	}
	private void processCommand(String cmd) {
		NoSqlEntityManager mgr = factory.createEntityManager();
		
		if("help".equals(cmd)) {
			println("Getting around");
			println("help;            Display this help");
			println("help <command>   Display command-specific help.");
			println("exit;            Exit this utility");
			println("");
			println("Commands:");
			println("SELECT           Selects dataset matching expression.  type 'help SELECT' for more info");
			println("PARTITIONS       Selects dataset matching expression in a partition.  type 'help PARTITIONS for more info");
			println("INDEXVIEW        Views an index.  type 'help INDEXVIEW' for more info");
			println("CREATE TABLE     Not in yet");
			println("INSERT           Not in yet");
		} else if("exit".equals(cmd)) {
			System.exit(0);
		} else if(startsWithIgnoreCase("help ", cmd)) {
			help.processHelpCmd(cmd);
		} else if(startsWithIgnoreCase("CREATE ", cmd)) {
			processCreate(cmd);
		} else if(startsWithIgnoreCase("SELECT ", cmd) || startsWithIgnoreCase("PARTITIONS ", cmd)) {
			select.processSelect(cmd, mgr);
		} else {
			throw new InvalidCommand();
		}
	}

	private boolean startsWithIgnoreCase(String target, String cmd) {
		int size = target.length();
		int min = Math.min(size, cmd.length());
		String partOfCmd = cmd.substring(0, min);
		if(target.equalsIgnoreCase(partOfCmd))
			return true;
		return false;
	}

	private void processCreate(String cmd) {
		String command = cmd.substring(7);
		String newCmd = command.trim();
		if(startsWithIgnoreCase("TABLE ", newCmd)) {
			processCreateTable(newCmd);
		} else {
			throw new InvalidCommand();
		}
	}

	private void processCreateTable(String newCmd) {
		String leftOver = newCmd.substring(6);
		String command = leftOver.trim();
		
		
	}


	
	private static void println(String msg) {
		System.out.println(msg);
	}
}
