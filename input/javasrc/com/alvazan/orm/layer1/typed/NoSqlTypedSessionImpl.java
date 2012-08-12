package com.alvazan.orm.layer1.typed;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.alvazan.orm.api.spi1.NoSqlTypedSession;
import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.IndexData;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi2.RowToPersist;
import com.alvazan.orm.api.spi2.TypedColumn;
import com.alvazan.orm.api.spi2.TypedRow;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;

public class NoSqlTypedSessionImpl implements NoSqlTypedSession {

	private NoSqlSession session;

	private DboDatabaseMeta metaInfo;
	/**
	 * To be removed eventually
	 * @param s
	 */
	@Deprecated
	public void setRawSession(NoSqlSession s) {
		this.session = s;
	}
	@Override
	@Deprecated
	public void setMetaInfo(DboDatabaseMeta meta) {
		this.metaInfo = meta;
	}
	
	@Override
	public NoSqlSession getRawSession() {
		return session;
	}
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public void put(String colFamily, TypedRow typedRow) {
		DboTableMeta metaClass = metaInfo.getMeta(colFamily);
		if(metaClass == null)
			throw new IllegalArgumentException("DboTableMeta for colFamily="+colFamily+" was not found");

		RowToPersist row = metaClass.translateToRow(typedRow);
		
		//This is if we need to be removing columns from the row that represents the entity in a oneToMany or ManyToMany
		//as the entity.accounts may have removed one of the accounts!!!
		if(row.hasRemoves())
			session.remove(metaClass.getColumnFamily(), row.getKey(), row.getColumnNamesToRemove());
		
		//NOW for index removals if any indexed values change of the entity, we remove from the index
		for(IndexData ind : row.getIndexToRemove()) {
			session.removeFromIndex(ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		
		//NOW for index adds, if it is a new entity or if values change, we persist those values
		for(IndexData ind : row.getIndexToAdd()) {
			session.persistIndex(ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		String cf = metaClass.getColumnFamily();
		byte[] key = row.getKey();
		List<Column> cols = row.getColumns();
		session.put(cf, key, cols);
	}
	
	@Override
	public void remove(String colFamily, TypedRow row) {
		
	}
	
	@Override
	public <T> void remove(String colFamily, T rowKey,
			Collection<byte[]> columnNames) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public <T> List<TypedRow<T>> find(String colFamily, List<T> rowKeys) {
		List<byte[]> rowKeysBytes = new ArrayList<byte[]>();

		DboTableMeta meta = metaInfo.getMeta(colFamily);
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
	public Iterable<Column> columnRangeScan(String colFamily, Object rowKey,
			Object from, Object to, int batchSize) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setOrmSessionForMeta(Object s) {
		session.setOrmSessionForMeta(s);
	}

}