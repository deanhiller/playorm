package com.alvazan.ssql.cmdline;


public class CmdHelp {

	void processHelpCmd(String cmd) {
		String oldCommand = cmd.substring(5);
		String command = oldCommand.trim();
		if("SELECT".equalsIgnoreCase(command)) {
			println("Select dataset matching expression in a table that is NOT partitioned at all");
			println("");
			println("Example: SELECT a FROM Activity as a LEFT JOIN a.trade as t WHERE a.numShares > 5");
			println("");
			println("In general, JQL(Java Query Language) can be followed.  We are adding standard SQL as well");			
		} else if("PARTITIONS".equalsIgnoreCase(command)) {
			println("Select dataset matching expression in a partition");
			println("");
			println("Example: PARTITIONS a('partition1') SELECT a FROM Activity as a LEFT JOIN a.trade as t WHERE a.numShares > 5");
			println("");
			println("In general, JQL(Java Query Language) can be followed.  We are adding standard SQL as well");
		} else if("VIEWINDEX".equalsIgnoreCase(command)){
			println("Lists the index nodes in the index the way they are stored(which is in order)");
			println("");
			println("Format(non-partitioned table): VIEWINDEX /<Column Family>/<Indexed Column>");
			println("Format(partitioned table): VIEWINDEX /<Column Family>/<Indexed Column>/<Partitioned by>/<Partition Id>");
			println("");
			println("Example: VIEWINDEX /Activity/trade/byAccount/56748321");
			println("");
		} else if("REINDEX".equalsIgnoreCase(command)) {
			println("Rebuilds an index.  This will remove duplicate rowkeys in an index AND if a row no longer exists it");
			println("will remove those index points as well BUT if you need to add missing index points, you need to map/reduce the");
			println("table and JUST read in every row and write it back out and it will index it.  NOTE: It is extremely tough to");
			println("be missing index points...duplicate rowkeys are more likely that missing points(we have had ZERO instances so far in production of this)");
			println("");
			println("Rebuild index(non-partitioned table): REINDEX /<Column Family>/<Indexed Column>");
			println("Rebuild index(partitioned table): REINDEX /<Column Family>/<Indexed Column>/<Partitioned by>/<Partition Id>");
			println("");
			println("Example 1: REINDEX /Activity/trade/byAccount/56748321");
			//println("Example 2: REINDEX /Activity/trade/byAccount/56748321 name");
			//println("         example 2 uses name index to rebuild the trade index in a partition");
			println("");
		} else if("LISTPARTITIONS".equalsIgnoreCase(command)) {
			println("Lists out all the partitions that you can query into for a Column Family OR use in the REINDEX or VIEWINDEX");
			println("");
			println("Usage: LISTPARTITIONS <Column Family> (Partitioned By)");
			println("");
			println("Example 1 is sufficient for most people...");
			println("Example 1(CF only partitioned one way): LISTPARTITIONS User");
			println("Example 2(CF multiple partitioned): LISTPARTITIONS PartitionedTrade account");
			println("     account is the field with the @NoSqlPartitionByThisField annotation");
		} else if(command.startsWith("CREATE ")) {
			processCreateHelp(command);
		} else if("INSERT".equalsIgnoreCase(command)) {
			System.out.println("not in yet");
		}
	}

	private void processCreateHelp(String command) {
		String subCmd = command.substring(7);
		String cmd = subCmd.trim();
		if(cmd.startsWith("TABLE ")) {
			String leftover = cmd.substring(6);
			String next = leftover.trim();
			int index = next.indexOf(" ");
			String tableName = next.substring(0, index-1);
			String rightSide = next.substring(index);
			String trimmedRight = rightSide.trim();
			if(!trimmedRight.startsWith("(") || !trimmedRight.endsWith(")"))
				throw new InvalidCommand("You are missing a left or right parens");
			String columns = trimmedRight.substring(1, trimmedRight.length()-1);
			String[] colAndTypes = columns.split(",");
			
			
		} else 
			throw new InvalidCommand();
	}

	private static void println(String msg) {
		System.out.println(msg);
	}
}
