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
		} else if("INDEXVIEW".equalsIgnoreCase(command)){
			println("Lists the index nodes in the index the way they are stored(which is in order)");
			println("");
			println("Format(non-partitioned table): INDEXVIEW /<Column Family>/<Indexed Column>");
			println("Format(partitioned table): INDEXVIEW /<Column Family>/<Indexed Column>/<Partitioned by>/<Partition Id>");
			println("");
			println("Example: INDEXVIEW /Activity/trade/byAccount/56748321");
			println("");
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
