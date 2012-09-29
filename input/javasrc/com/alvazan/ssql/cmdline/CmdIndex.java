package com.alvazan.ssql.cmdline;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z5api.IndexPoint;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class CmdIndex {

	public void processIndex(String cmd, NoSqlEntityManager mgr) {
		String oldCommand = cmd.substring(10);
		String command = oldCommand.trim();

		int index = command.indexOf("/");
		if(index != 0) {
			throw new InvalidCommand("Index must start with '/' and does not");
		}
		String withoutSlash = command.substring(1);
		index = withoutSlash.indexOf("/");
		if(index < 0) {
			throw new InvalidCommand("Index requires two '/'");
		}

		String cf = withoutSlash.substring(0, index);
		String lastPart = withoutSlash.substring(index+1);
		goMore(mgr, cf, lastPart);
	}

	private void goMore(NoSqlEntityManager mgr, String cf, String lastPart) {
		int index = lastPart.indexOf("/");
		String field = null;
		String partitionBy = null;
		String partitionId = null;
		if(index < 0) {
			field = lastPart;
		} else {
			field = lastPart.substring(0, index);
			String partitionPart = lastPart.substring(index);
			index = partitionPart.indexOf("/");
			if(index < 0)
				throw new InvalidCommand("Must have two or four '/' characters");

			partitionBy = partitionPart.substring(0, index);
			partitionId = partitionPart.substring(index);
		}
		
		DboTableMeta meta = mgr.find(DboTableMeta.class, cf);
		if(meta == null) {
			throw new InvalidCommand("Column family meta not found="+cf);
		}
		
		DboColumnMeta colMeta = meta.getColumnMeta(field);
		if(colMeta == null) {
			throw new InvalidCommand("Column="+field+" not found on table="+cf);
		}
		
		NoSqlTypedSession s = mgr.getTypedSession();
		Cursor<IndexPoint> indexView = s.indexView(cf, field, partitionBy, partitionId);
		
		System.out.println("indexed value type="+colMeta.getStorageType());
		System.out.println("row key type="+meta.getIdColumnMeta().getStorageType());
		System.out.println("<indexed value>.<row key>");
		
		int count = 0;
		while(indexView.next()) {
			IndexPoint current = indexView.getCurrent();
			String indVal = current.getIndexedValueAsString();
			if(indVal == null)
				indVal = "";
			String key = current.getKeyAsString();
			System.out.println(indVal+"."+key);
			count++;
		}
		System.out.println(count+" Columns Total");
	}
}
