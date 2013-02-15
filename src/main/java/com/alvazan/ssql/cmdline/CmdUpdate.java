package com.alvazan.ssql.cmdline;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.exc.ParseException;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;

public class CmdUpdate {

	void processUpdate(String cmd, NoSqlEntityManager mgr) {
		NoSqlTypedSession s = mgr.getTypedSession();
		try {
			int count = s.executeQuery(cmd);
			mgr.flush();
			println(count + " row updated");
		} catch(ParseException e) {
			Throwable childExc = e.getCause();
			throw new InvalidCommand("Scalable-SQL command was invalid.  Reason="+childExc.getMessage()+" AND you may want to add -v option to playcli to get more info", e);
		}
	}

	private void println(String msg) {
		System.out.println(msg);
	}
}
