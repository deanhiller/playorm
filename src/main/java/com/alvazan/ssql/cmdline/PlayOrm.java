package com.alvazan.ssql.cmdline;


import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import jline.ConsoleReader;
import jline.History;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;


public class PlayOrm {

	private static final Logger log = LoggerFactory.getLogger(PlayOrm.class);
	private NoSqlEntityManagerFactory factory;
	private CmdHelp help = new CmdHelp();
	private CmdSelect select = new CmdSelect();
	private CmdIndex index = new CmdIndex();
	private CmdUpdate update = new CmdUpdate();
	private CmdDelete delete = new CmdDelete();
	public final static String HISTORYFILE = ".playorm.history";

	private CmdListPartitions partitions = new CmdListPartitions();
	
	public PlayOrm(NoSqlEntityManagerFactory factory) {
		this.factory = factory;
	}

	public static void main(String[] args) {
		
//		String hex = "44626f5461626c654d6574613a636f6c756d6e46616d696c79";
//		byte[] d = Hex.decodeHex(hex.toCharArray());
//		String value = StandardConverters.convertToString(String.class, d);
//				
//		log.info("val="+ value);
//		byte[] data = StandardConverters.convertToBytes("ejktest001:time");
//		char[] str = Hex.encodeHex(data);
//		String s = new String(str);
//		log.info("s="+s);
		
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
		if(storeType == DbTypeEnum.CASSANDRA) {
			String[] vals = bean.getSeeds().split(":");
			if(vals.length == 2) {
				log.info("setting port to="+vals[1]);
				properties.put(Bootstrap.CASSANDRA_THRIFT_PORT, vals[1]);
			}
			
			//default to quorom now...
			properties.put(Bootstrap.CASSANDRA_DEFAULT_CONSISTENCY_LEVEL, "CL_QUORUM");
			if(bean.getConsistencyLevel() != null) {
				properties.put(Bootstrap.CASSANDRA_DEFAULT_CONSISTENCY_LEVEL, bean.getConsistencyLevel());
			}
			Bootstrap.createAndAddBestCassandraConfiguration(properties , "", bean.getKeyspace(), bean.getSeeds());
		}
		else if (storeType == DbTypeEnum.MONGODB) {
			Bootstrap.createAndAddBestMongoDbConfiguration(properties ,"", bean.getKeyspace(), bean.getSeeds());
		}
		else if (storeType == DbTypeEnum.HBASE) {
			Bootstrap.createAndAddBestHBaseConfiguration(properties ,"", bean.getKeyspace(), bean.getSeeds());
		}
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
		try {
			ConsoleReader reader = new ConsoleReader();
			reader.setBellEnabled(false);
			String historyFile = System.getProperty("user.home")
					+ File.separator + HISTORYFILE;
			History history = new History(new File(historyFile));
			reader.setHistory(history);

			String prompt;
			String line = "";
			String currentStatement = "";
			boolean inCompoundStatement = false;

			while (line != null) {
				prompt = (inCompoundStatement) ? "...\t" : "playorm >>";

				line = reader.readLine(prompt);

				if (line == null)
					return;

				line = line.trim();

				// skipping empty and comment lines
				if (line.isEmpty() || line.startsWith("--"))
					continue;

				currentStatement += line;

				if (line.endsWith(";") || line.equals("?")) {
					try {
						process(currentStatement);
					} catch (Exception exp) {
						if (log.isWarnEnabled())
							log.warn("Exception occurred", exp);
						println("Sorry, we ran into a bug, recovering now so you can continue using command line");
						println("playorm >> ");
					}
					currentStatement = "";
					inCompoundStatement = false;
				} else {
					currentStatement += " "; // ready for new line
					inCompoundStatement = true;
				}
			}
		} catch (IOException exp) {
			if (log.isWarnEnabled())
				log.warn("Exception occurred", exp);
		}
	}

	private void process(String newLine) {
		if(log.isDebugEnabled())
			log.debug("processing line="+newLine);
		String[] commands = newLine.split(";");
		
		for(String cmd : commands) {
			try {
				String justCmd = cmd.trim();
				processCommand(justCmd);
			} catch (InvalidCommand e) {
				if (log.isInfoEnabled())
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
			println("UPDATE           Updates dataset matching expression.  type 'help UPDATE' for more info");
			println("DELETE           Delete  dataset matching expression.  type 'help DELETE' for more info");
			println("DELETECOLUMN     Delete a column from the rows which matching expression.  type 'help DELETECOLUMN' for more info");
			println("PARTITIONS       Selects dataset matching expression in a partition.  type 'help PARTITIONS for more info");
			println("VIEWINDEX        Views an index.  type 'help VIEWINDEX' for more info");
			println("REINDEX          Rebuild a particular index.  type 'help REINDEX' for more info");
			println("CREATE TABLE     Not in yet");
			println("INSERT           Not in yet");
			println("LIST TABLES      To list all non-virtual column families. To list virual column families use LIST TABLES -all");
			println("LISTPARTITIONS   IF you partition on a field with @ManyToOne, you can list the partitions. type 'help LISTPARTITIONS' for more info");
		} else if("exit".equals(cmd)) {
			System.exit(0);
		} else if(startsWithIgnoreCase("help ", cmd)) {
			help.processHelpCmd(cmd);
		} else if(startsWithIgnoreCase("CREATE ", cmd)) {
			processCreate(cmd);
		} else if(startsWithIgnoreCase("SELECT ", cmd) || startsWithIgnoreCase("PARTITIONS ", cmd)) {
			select.processSelect(cmd, mgr);
		} else if(startsWithIgnoreCase("VIEWINDEX ", cmd)) {
			index.processIndex(cmd, mgr);
		} else if (startsWithIgnoreCase("UPDATE ", cmd)) {
			update.processUpdate(cmd, mgr);
		} else if (startsWithIgnoreCase("DELETE ", cmd)) {
		    delete.processDelete(cmd, mgr);
		} else if (startsWithIgnoreCase("DELETECOLUMN ", cmd)) {
			delete.processDeleteColumn(cmd, mgr);
		} else if(startsWithIgnoreCase("REINDEX ", cmd)) {
			index.reindex(cmd, mgr);
		} else if(startsWithIgnoreCase("LISTPARTITIONS ", cmd)) {
			partitions.list(cmd, mgr);
		} else if(startsWithIgnoreCase("LIST TABLES", cmd)) {
			listTables(cmd, mgr);
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

	private void listTables(String cmd, NoSqlEntityManager mgr) {
		DboDatabaseMeta database = mgr.find(DboDatabaseMeta.class,
				DboDatabaseMeta.META_DB_ROWKEY);
		Collection<DboTableMeta> allTables = database.getAllTables();
		int count = 0;
		if (cmd.contains("-all") || cmd.contains("-ALL")) {
			for (DboTableMeta tableMeta : allTables) {
				println(tableMeta.getColumnFamily());
				count++;
			}
		} else {
			for (DboTableMeta tableMeta : allTables) {
				if (!tableMeta.isVirtualCf()) {
					println(tableMeta.getColumnFamily());
					count++;
				}
			}
		}
		println("");
		println(count + " tables");
		println("");
	}

	private static void println(String msg) {
		System.out.println(msg);
	}

}
