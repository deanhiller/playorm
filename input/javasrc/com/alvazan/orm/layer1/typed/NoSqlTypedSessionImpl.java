package com.alvazan.orm.layer1.typed;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.alvazan.orm.api.spi1.NoSqlTypedSession;
import com.alvazan.orm.api.spi1.TypedColumn;
import com.alvazan.orm.api.spi1.TypedRow;
import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;

public class NoSqlTypedSessionImpl implements NoSqlTypedSession {

	private NoSqlSession session;

	/**
	 * To be removed eventually
	 * @param s
	 */
	@Deprecated
	public void setRawSession(NoSqlSession s) {
		this.session = s;
	}
	@Override
	public NoSqlSession getRawSession() {
		return session;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void persist(DboTableMeta meta, TypedRow row) {
		byte[] rowKey = meta.getIdColumnMeta().convertToStorage2(row.getRowKey());
		Collection<TypedColumn> cols = row.getColumnsAsColl();
		List<Column> theCols = new ArrayList<Column>();
		for(TypedColumn c : cols) {
			Column theCol = new Column();
			DboColumnMeta columnMeta = meta.getColumnMeta(c.getName());
			byte[] value = columnMeta.convertToStorage2(c.getValue());
			theCol.setName(columnMeta.getColumnNameAsBytes());
			theCol.setValue(value);
			theCols.add(theCol);
		}
		
		session.persist(meta.getColumnFamily(), rowKey, theCols);
	}
	
	@Override
	public void remove(DboTableMeta meta, TypedRow row) {
		
	}
	
	@Override
	public <T> void remove(DboTableMeta meta, T rowKey,
			Collection<byte[]> columnNames) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public <T> List<TypedRow<T>> find(DboTableMeta meta, List<T> rowKeys) {
		List<byte[]> rowKeysBytes = new ArrayList<byte[]>();

		DboColumnMeta idMeta = meta.getIdColumnMeta();
		for(T k : rowKeys) {
			byte[] rowK = idMeta.convertToStorage2(k);
			rowKeysBytes.add(rowK);
		}
		
		List<Row> rows = session.find(meta.getColumnFamily(), rowKeysBytes);

		List<TypedRow<T>> result = new ArrayList<TypedRow<T>>();
		for(Row r : rows) {
			TypedRow<T> typed = new TypedRow<T>();
			Object obj = idMeta.convertFromStorage2(r.getKey());
			typed.setRowKey((T) obj);

			for(DboColumnMeta colMeta : meta.getAllColumns()) {
				TypedColumn col = new TypedColumn();
				
				Column c = r.getColumn(colMeta.getColumnNameAsBytes());
				
				typed.addColumn(col);
			}
				
			result.add(typed);
		}
		throw new UnsupportedOperationException("need to complete this here");
	}
	
	private String convert(byte[] name) {
		try {
			return new String(name, "UTF8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void flush() {
		session.flush();
	}
	@Override
	public void clearDatabase() {
		session.clearDb();
	}
	
	@Override
	public Iterable<Column> columnRangeScan(DboTableMeta meta, Object rowKey,
			Object from, Object to, int batchSize) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setOrmSessionForMeta(Object s) {
		session.setOrmSessionForMeta(s);
	}

}