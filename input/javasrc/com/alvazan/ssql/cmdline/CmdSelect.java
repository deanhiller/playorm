package com.alvazan.ssql.cmdline;

import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.exc.ParseException;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z3api.QueryResult;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToManyMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedColumn;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class CmdSelect {

	void processSelect(String cmd, NoSqlEntityManager mgr) {
		NoSqlTypedSession s = mgr.getTypedSession();
		try {
			QueryResult result = s.createQueryCursor(cmd, 100);
			Cursor<List<TypedRow>> cursor = result.getAllViewsCursor();

			processBatch(cursor, result.getViews());
			
		} catch(ParseException e) {
			Throwable childExc = e.getCause();
			throw new InvalidCommand("Scalable-SQL command was invalid.  Reason="+childExc.getMessage()+" AND you may want to add -v option to playcli to get more info", e);
		}
	}

	/**
	 * @param cursor
	 * @param views 
	 * @return whether we have exhausted the cursor or not.
	 */
	private void processBatch(Cursor<List<TypedRow>> cursor, List<ViewInfo> views) {
		int rowCount = 0;
		while(cursor.next()) {
			List<TypedRow> joinedRow = cursor.getCurrent();
			printJoinedRow(rowCount, joinedRow);
			rowCount++;
		}
		println(rowCount+" Rows returned");
	}

	private void printJoinedRow(int rowCount, List<TypedRow> joinedRow) {
		println("----------- JoinedRow"+rowCount);
		for(TypedRow r: joinedRow) {
			ViewInfo view = r.getView();
			DboTableMeta meta = view.getTableMeta();
			DboColumnIdMeta idColumnMeta = meta.getIdColumnMeta();
			String columnName = idColumnMeta.getColumnName();
			
			println("RowKey:"+r.getRowKeyString()+" ("+columnName+")");
			printColumns(r, meta);
		}
	}

	private void printColumns(TypedRow r, DboTableMeta meta) {
		for(TypedColumn c : r.getColumnsAsColl()) {
			DboColumnMeta colMeta = meta.getColumnMeta(c.getName());
			if(colMeta != null) {
				String name = c.getName();
			
				if(colMeta instanceof DboColumnToManyMeta) {
					String subName = c.getCompositeSubName();
					String fullName = name+"."+subName;
					byte[] value = (byte[]) c.getValue();
					String strVal = StandardConverters.convertToString(byte[].class, value);
					println("=> "+fullName+" = "+ strVal);
				} else {
					String val = c.getValueAsString();
					println("=> "+name+" = "+val);
				}
			} else {
				throw new RuntimeException("we need to fix this");
			}
		}
	}
	
	private void println(String msg) {
		System.out.println(msg);
	}
}
