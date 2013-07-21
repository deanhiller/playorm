package com.alvazan.ssql.cmdline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z3api.QueryResult;
import com.alvazan.orm.api.z5api.IndexPoint;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedColumn;
import com.alvazan.orm.api.z8spi.meta.TypedRow;

public class CmdIndex {
	private static final Logger log = LoggerFactory.getLogger(CmdIndex.class);
	private static final int BATCH_SIZE = 200;
	private Integer totalRowCount = null;
	//private static final int TIME_TO_REPORT = 10000;
	
	public void reindex(String cmd, NoSqlEntityManager mgr) {
		String oldCommand = cmd.substring(8);
		String command = oldCommand.trim();
		ColFamilyData data = parseData(mgr, command);
		
		NoSqlTypedSession s = mgr.getTypedSession();
		String cf = data.getColFamily();
		String field = data.getColumn();
		String by = data.getPartitionBy();
		String id = data.getPartitionId();
        DboTableMeta meta = data.getTableMeta();
        DboColumnMeta colMeta = data.getColumnMeta();
        if (!colMeta.isIndexed()) {
            // check if it was indexed earlier and now not indexed. If yes, then remove the indices.
            // Issue #120
            System.out.println("The column " + field + " is not indexed");
            ScanInfo info = ScanInfo.createScanInfo(colMeta, by, id);
            DboTableMeta indexTableMeta = mgr.find(DboTableMeta.class, info.getIndexColFamily());
            System.out.println("Wait...we are checking if it was indexed earlier and removing all its old indexes.");
            mgr.getSession().remove(indexTableMeta, info.getRowKey());
            s.flush();
            return;
        }

		Cursor<IndexPoint> indexView = s.indexView(cf, field, by, id);
		Cursor<IndexPoint> indexView2 = s.indexView(cf, field, by, id);
		
		System.out.println("indexed value type="+colMeta.getStorageType());
		System.out.println("row key type="+meta.getIdColumnMeta().getStorageType());
		System.out.println("It is safe to kill this process at any time since it only removes duplicates");
		System.out.println("Beginning re-index");

		int totalChanges = 0;
		int rowCountProcessed = 0;
		while(true) {
			Map<Object, KeyValue<TypedRow>> keyToRow = findNextSetOfData(s, cf, indexView);
			rowCountProcessed += keyToRow.size();
			if(keyToRow.size() == 0) {
				break; //finished
			}
			
			Counter c = processAllColumns(s, data, keyToRow, indexView2);
			totalChanges += c.getChangedCounter();
			
			if(rowCountProcessed % 1000 == 0) {
				System.out.println("#Rows processed="+rowCountProcessed+" totalRows to process="+totalRowCount+" totalChanges so far="+totalChanges);
			}
		}
		
		System.out.println("#Rows processed="+rowCountProcessed+" totalRows to process="+totalRowCount+" totalChanges="+totalChanges);
	}

	private Counter processAllColumns(NoSqlTypedSession s, ColFamilyData data, Map<Object, KeyValue<TypedRow>> keyToRow,
			Cursor<IndexPoint> indexView2) {
		String colName = data.getColumn();
		
		indexView2.beforeFirst();
		int rowCounter = 0;
		int changedCounter = 0;
		while(indexView2.next()) {
			rowCounter++;
			IndexPoint pt = indexView2.getCurrent();
			
			KeyValue<TypedRow> row = keyToRow.get(pt.getKey());
			if(row == null) {
				if(log.isDebugEnabled())
					log.debug("row is null for key="+pt.getKey());
				//We are iterating over two views in batch mode soooo
				//one batch may not have any of the keys of the other batch.  This is very normal
			} else if(row.getException() != null || row.getValue() == null) {
				removeIndexPt(s, data, pt);
				changedCounter++;
			} else {
				TypedRow val = row.getValue();
				if (processColumn(s, data, val, pt)) {
					changedCounter++;
				}
			}

			if(changedCounter > 50) {
				s.flush();
				//System.out.println("Successfully flushed all previous changes.  row="+rowCounter);
			}
			if(rowCounter % 20000 == 0) {
				System.out.println("reindexing.  row count so far="+rowCounter+" num index points changed="+changedCounter);
			}
		}
		
		if(totalRowCount == null)
			totalRowCount = rowCounter;
		s.flush();
		return new Counter(rowCounter, changedCounter);
	}

	private boolean processColumn(NoSqlTypedSession s, ColFamilyData data,TypedRow typedRow,
			IndexPoint pt) {
		String colName = data.getColumn();
		TypedColumn column = typedRow.getColumn(colName);
		if (column == null) {
			//It means column was deleted by user. Doing nothing as of now
			return false;
		}
		else {
			Object value = column.getValue();
			DboColumnMeta colMeta = data.getColumnMeta();
			if (value == null && colMeta instanceof DboColumnToOneMeta) {
				DboColumnToOneMeta one = (DboColumnToOneMeta) colMeta;
				value = one.convertFromStorage2(column.getCompositeSubName());
			}
			if(!valuesEqual(pt.getIndexedValue(), value)) {
				System.out.println("Entity with rowkey="+pt.getKeyAsString()+" has extra incorrect index point with value="+pt.getIndexedValueAsString()+" correct value should be= "+value);
				s.removeIndexPoint(pt, data.getPartitionBy(), data.getPartitionId());
				IndexColumn col = new IndexColumn();
				col.setColumnName(colName);
				col.setPrimaryKey(pt.getRawKey());
				byte[] indValue = StandardConverters.convertToBytes(value);
				col.setIndexedValue(indValue);
				IndexPoint newPoint = new IndexPoint(pt.getRowKeyMeta(), col,data.getColumnMeta());
				s.addIndexPoint(newPoint, data.getPartitionBy(), data.getPartitionId());
				return true;
			}
		}
		return false;
	}

	private void removeIndexPt(NoSqlTypedSession s, ColFamilyData data,
			IndexPoint pt) {
		System.out.println("Entity with rowkey="+pt.getKeyAsString()+" does not exist, WILL remove from index");
		s.removeIndexPoint(pt, data.getPartitionBy(), data.getPartitionBy());
	}

	private boolean valuesEqual(Object indexedValue, Object value) {
		if(indexedValue == null) {
			if(value == null)
				return true;
			return false;
		} else if(indexedValue.equals(value))
			return true;
		
		return false;
	}

	private static class Counter {
		private int rowCounter;
		private int changedCounter;
		public Counter(int rowCounter, int changedCounter) {
			super();
			this.rowCounter = rowCounter;
			this.changedCounter = changedCounter;
		}
		public int getRowCounter() {
			return rowCounter;
		}
		public int getChangedCounter() {
			return changedCounter;
		}
	}
	
	private Map<Object, KeyValue<TypedRow>> findNextSetOfData(
			NoSqlTypedSession s, String cf, Cursor<IndexPoint> indexView) {
		int batchCounter = 0;
		List<Object> keys = new ArrayList<Object>(); 
		while(batchCounter < BATCH_SIZE && indexView.next()) {
			IndexPoint current = indexView.getCurrent();
			keys.add(current.getKey());
			batchCounter++;
		}
		
		Map<Object, KeyValue<TypedRow>> keyToRow = new HashMap<Object, KeyValue<TypedRow>>();
		Cursor<KeyValue<TypedRow>> cursor = s.createFindCursor(cf, keys, BATCH_SIZE);
		while(cursor.next()) {
			KeyValue<TypedRow> current = cursor.getCurrent();
			keyToRow.put(current.getKey(), current);
		}
		return keyToRow;
	}
	
	public void processIndex(String cmd, NoSqlEntityManager mgr) {
		String oldCommand = cmd.substring(10);
		String command = oldCommand.trim();
		
		ColFamilyData data = parseData(mgr, command);
		NoSqlTypedSession s = mgr.getTypedSession();
		
		String cf = data.getColFamily();
		String field = data.getColumn();
		String by = data.getPartitionBy();
		String id = data.getPartitionId();

		Cursor<IndexPoint> indexView = s.indexView(cf, field, by, id);

		DboTableMeta meta = data.getTableMeta();
		DboColumnMeta colMeta = data.getColumnMeta();
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
			System.out.println(count+" "+indVal+"."+key);
			count++;
		}
		System.out.println(count+" Columns Total");	
	}
	
	public ColFamilyData parseData(NoSqlEntityManager mgr, String command) {
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
		return goMore(mgr, cf, lastPart);
	}

	private ColFamilyData goMore(NoSqlEntityManager mgr, String cf, String lastPart) {
		ColFamilyData data = new ColFamilyData();
		
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
		if (meta == null) {
			System.out.println("Column family meta not found for " + cf);
			System.out.println("You can select from following tables:");
			QueryResult result = mgr.getTypedSession().createQueryCursor("select * from DboTableMeta", 100);
			Cursor<List<TypedRow>> cursor = result.getAllViewsCursor();
			while (cursor.next()) {
				List<TypedRow> joinedRow = cursor.getCurrent();
				for (TypedRow r : joinedRow) {
					System.out.println(r.getRowKeyString());
				}
			}
			System.out.println("");
			throw new InvalidCommand("Column family meta not found for " + cf);
		}
		
		DboColumnMeta colMeta = meta.getColumnMeta(field);
		if(colMeta == null) {
			colMeta = meta.getIdColumnMeta();
			if(!(colMeta != null && colMeta.getColumnName().equals(field))) {
				System.out.println("Column= "+field+" not found on table "+cf);
				System.out.println("You can view index for following columns:");
				for(DboColumnMeta colMetaOther : meta.getIndexedColumns()) {
					System.out.println(colMetaOther.getColumnName());
				}
				System.out.println("");
				throw new InvalidCommand("Column= "+field+" not found on table "+cf);
			}
		}
		
		data.setColFamily(cf);
		data.setColumn(field);
		data.setPartitionBy(partitionBy);
		data.setPartitionId(partitionId);
		data.setTableMeta(meta);
		data.setColumnMeta(colMeta);
		
		return data;
	}

}
