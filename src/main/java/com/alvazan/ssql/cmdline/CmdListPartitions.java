package com.alvazan.ssql.cmdline;

import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z3api.QueryResult;
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.IndexPoint;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class CmdListPartitions {

	public void list(String cmd, NoSqlEntityManager mgr) {
		String lastPart = cmd.substring(15).trim();
		String[] pieces = lastPart.split(" ");
		if(pieces.length == 0)
			throw new InvalidCommand("not enough arguments");
		else if(pieces.length > 2)
			throw new InvalidCommand("too many arguments '"+lastPart+"'");
		
		String colFamily = pieces[0];
		String field = null;
		if(pieces.length > 1) 
			field = pieces[1];
		
		listPartitions(mgr, colFamily, field);
	}

	private void listPartitions(NoSqlEntityManager mgr, String colFamily,
			String field) {
		DboTableMeta meta = mgr.find(DboTableMeta.class, colFamily);
		if(meta == null)
			throw new InvalidCommand("This column family='"+colFamily+"' does not exist");
		List<DboColumnMeta> partitionedCols = meta.getPartitionedColumns();
		if(partitionedCols.size() == 0) {
			throw new InvalidCommand("This column family='"+colFamily+"' is not partitioned");
		} else if(field == null) {
			if(partitionedCols.size() > 1)
				throw new InvalidCommand("You must supply a column as this table is partitioned by multiple columns");
			DboColumnMeta colMeta = partitionedCols.get(0);
			listByPartition(colMeta, mgr);
			return;
		} 

		for(DboColumnMeta col : partitionedCols) {
			if(field.equals(col.getColumnName())) {
				listByPartition(col, mgr);
				return;
			}
		}
		
		throw new InvalidCommand("ColumnFamily="+colFamily+" is not partitioned by column='"+field+"'");
	}

	private void listByPartition(DboColumnMeta colMeta, NoSqlEntityManager mgr) {
		if(!(colMeta instanceof DboColumnToOneMeta))
			throw new InvalidCommand("We can only list the partitions when partitioning by a field with @ManyToOne annotation and col="+colMeta.getColumnName()+" does not have that annotation");
		String cf = colMeta.getOwner().getColumnFamily();
		
		DboColumnToOneMeta fkMeta = (DboColumnToOneMeta) colMeta;
		DboTableMeta fkTable = fkMeta.getFkToColumnFamily();
		
		String sql = "SELECT * FROM "+fkTable.getColumnFamily();
		NoSqlTypedSession s = mgr.getTypedSession();
		QueryResult query = s.createQueryCursor(sql, 100);
		ViewInfo oneView = query.getViews().get(0);
		
		Cursor<IndexColumnInfo> cursor = query.getCursor();

		String firstPart = "/"+cf+"/<AnyIndexedColumn>/"+fkMeta.getColumnName()+"/";

		System.out.println("Printing partitions of "+cf+" by column "+colMeta.getColumnName());
		System.out.println("");
		//always print the nullpartition first and start count at with 1...
		System.out.println("/"+cf+"/<AnyIndexedColumn>");
		
		int counter = 1;
		while(cursor.next()) {
			IndexColumnInfo current = cursor.getCurrent();
			IndexPoint pt = current.getKeyForView(oneView);
			String key = pt.getKeyAsString();
			System.out.println(firstPart+key);
			counter++;
		}
		System.out.println(counter+" Total Partitions");
	}
}
