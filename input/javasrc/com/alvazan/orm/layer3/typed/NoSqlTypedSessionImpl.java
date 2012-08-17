package com.alvazan.orm.layer3.typed;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.spi3.NoSqlTypedSession;
import com.alvazan.orm.api.spi3.TypedRow;
import com.alvazan.orm.api.spi3.meta.DboColumnIdMeta;
import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi3.meta.IndexData;
import com.alvazan.orm.api.spi3.meta.MetaAndIndexTuple;
import com.alvazan.orm.api.spi3.meta.MetaQuery;
import com.alvazan.orm.api.spi3.meta.NoSqlSessionFactory;
import com.alvazan.orm.api.spi3.meta.RowToPersist;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi5.SpiQueryAdapter;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.Row;

@SuppressWarnings("rawtypes")
public class NoSqlTypedSessionImpl implements NoSqlTypedSession {

	@Inject
	private NoSqlSessionFactory noSqlSessionFactory;
	@Inject
	private CachedMeta cachedMeta;
	
	private NoSqlSession session;
	
	/**
	 * To be removed eventually
	 * @param s
	 */
	public void setInformation(NoSqlSession s) {
		this.session = s;
	}
	
	@Override
	public NoSqlSession getRawSession() {
		return session;
	}

	@Override
	public void put(String colFamily, TypedRow typedRow) {
		DboTableMeta metaClass = cachedMeta.getMeta(colFamily);
		if(metaClass == null)
			throw new IllegalArgumentException("DboTableMeta for colFamily="+colFamily+" was not found");

		RowToPersist row = metaClass.translateToRow(typedRow);
		
		//This is if we need to be removing columns from the row that represents the entity in a oneToMany or ManyToMany
		//as the entity.accounts may have removed one of the accounts!!!
		if(row.hasRemoves())
			session.remove(metaClass.getColumnFamily(), row.getKey(), row.getColumnNamesToRemove());
		
		String cf = metaClass.getColumnFamily();
		//NOW for index removals if any indexed values change of the entity, we remove from the index
		for(IndexData ind : row.getIndexToRemove()) {
			session.removeFromIndex(cf, ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		
		//NOW for index adds, if it is a new entity or if values change, we persist those values
		for(IndexData ind : row.getIndexToAdd()) {
			session.persistIndex(cf, ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		
		byte[] key = row.getKey();
		List<Column> cols = row.getColumns();
		session.put(cf, key, cols);
	}
	
	@Override
	public void remove(String colFamily, TypedRow row) {
		throw new UnsupportedOperationException("not done yet");
	}
	
	@Override
	public <T> TypedRow<T> find(String cf, T id) {
		List<T> keys = new ArrayList<T>();
		keys.add(id);
		List<KeyValue<TypedRow<T>>> rows = findAll(cf, keys);
		return rows.get(0).getValue();
	}
	
	@Override
	public <T> List<KeyValue<TypedRow<T>>> findAll(String colFamily, List<T> keys) {
		if(keys == null)
			throw new IllegalArgumentException("keys list cannot be null");
		DboTableMeta meta = cachedMeta.getMeta(colFamily);
		if(meta == null)
			throw new IllegalArgumentException("Meta for columnfamily="+colFamily+" was not found");

		List<byte[]> noSqlKeys = new ArrayList<byte[]>();
		DboColumnMeta idMeta = meta.getIdColumnMeta();
		for(T k : keys) {
			byte[] rowK = idMeta.convertToStorage2(k);
			noSqlKeys.add(rowK);
		}
		
		return findAllImpl(meta, keys, noSqlKeys, null);
	}

	<T> List<KeyValue<TypedRow<T>>> findAllImpl(DboTableMeta meta, List<T> keys, List<byte[]> noSqlKeys, String indexName) {
		//NOTE: It is WAY more efficient to find ALL keys at once then it is to
		//find one at a time.  You would rather have 1 find than 1000 if network latency was 1 ms ;).
		String cf = meta.getColumnFamily();
		List<Row> rows = session.find(cf, noSqlKeys);
		return getKeyValues(meta, keys, noSqlKeys, rows, indexName);
	}
	
	private <T> List<KeyValue<TypedRow<T>>> getKeyValues(DboTableMeta meta, List<T> keys,List<byte[]> noSqlKeys,List<Row> rows, String indexName){
		List<KeyValue<TypedRow<T>>> keyValues = new ArrayList<KeyValue<TypedRow<T>>>();

		if(keys != null)
			translateRows(meta, keys, rows, keyValues);
		else
			translateRowsForQuery(meta, noSqlKeys, rows, keyValues, indexName);
		
		return keyValues;
	}

	@SuppressWarnings("unchecked")
	private <T> void translateRowsForQuery(DboTableMeta meta, List<byte[]> noSqlKeys, List<Row> rows, List<KeyValue<TypedRow<T>>> keyValues, String indexName) {
		for(int i = 0; i < rows.size(); i++) {
			Row row = rows.get(i);
			byte[] rowKey = noSqlKeys.get(i);
			DboColumnIdMeta idField = meta.getIdColumnMeta();
			T key = (T) idField.convertFromStorage2(rowKey);
			
			KeyValue<TypedRow<T>> keyVal;
			if(row == null) {
				keyVal = new KeyValue<TypedRow<T>>();
				keyVal.setKey(key);
				RowNotFoundException exc = new RowNotFoundException("Your query="+indexName+" contained a value with a pk where that entity no longer exists in the nosql store");
				keyVal.setException(exc);
			} else {
				keyVal = meta.translateFromRow(row);
			}
			
			keyValues.add(keyVal);
		}		
	}
	
	private <T> void translateRows(DboTableMeta meta,
			List<T> keys, List<Row> rows,
			List<KeyValue<TypedRow<T>>> keyValues) {
		for(int i = 0; i < rows.size(); i++) {
			Row row = rows.get(i);
			T key = keys.get(i);
			
			KeyValue<TypedRow<T>> keyVal;
			if(row == null) {
				keyVal = new KeyValue<TypedRow<T>>();
				keyVal.setKey(key);
			} else {
				keyVal = meta.translateFromRow(row);
			}
			
			keyValues.add(keyVal);
		}
	}
	
	@Override
	public void flush() {
		session.flush();
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<KeyValue<TypedRow>> runQuery(String query) {
		MetaAndIndexTuple tuple = noSqlSessionFactory.parseQueryForAdHoc(query);
		MetaQuery metaQuery = tuple.getMetaQuery();
		SpiQueryAdapter spiQueryAdapter = metaQuery.createSpiMetaQuery(null, null, session);
		
		List<byte[]> primaryKeys = spiQueryAdapter.getResultList();
		
		DboTableMeta meta = metaQuery.getTargetTable();
		List rows = this.findAllImpl(meta, null, primaryKeys, null);
		return rows;
	}
}