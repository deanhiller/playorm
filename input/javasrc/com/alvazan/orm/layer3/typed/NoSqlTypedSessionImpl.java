package com.alvazan.orm.layer3.typed;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z5api.QueryParser;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z5api.SpiQueryAdapter;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.parser.antlr.ViewInfo;

@SuppressWarnings("rawtypes")
public class NoSqlTypedSessionImpl implements NoSqlTypedSession {

	@Inject
	private QueryParser noSqlSessionFactory;
	@Inject
	private CachedMeta cachedMeta;
	
	private NoSqlSession session;
	
	/**
	 * NOTE: This must be here so that if you get TypedSession from the NoSqlEntitManager, he will
	 * have the same session object and flush flushes both typed and ORM data at the same time!!!
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
		List<KeyValue<TypedRow<T>>> rows = findAllList(cf, keys);
		return rows.get(0).getValue();
	}
	
	@Override
	public <T> Cursor<KeyValue<TypedRow<T>>> findAll2(String colFamily, Iterable<T> keys) {
		if(keys == null)
			throw new IllegalArgumentException("keys list cannot be null");
		DboTableMeta meta = cachedMeta.getMeta(colFamily);
		if(meta == null)
			throw new IllegalArgumentException("Meta for columnfamily="+colFamily+" was not found");
		DboColumnMeta idMeta = meta.getIdColumnMeta();
		Iterable<byte[]> noSqlKeys = new IterableTypedProxy<T>(idMeta, keys);
		return findAllImpl2(meta, keys, noSqlKeys, null);
	}

	<T> Cursor<KeyValue<TypedRow<T>>> findAllImpl2(DboTableMeta meta, Iterable<T> keys, Iterable<byte[]> noSqlKeys, String indexName) {
		//NOTE: It is WAY more efficient to find ALL keys at once then it is to
		//find one at a time.  You would rather have 1 find than 1000 if network latency was 1 ms ;).
		String cf = meta.getColumnFamily();
		AbstractCursor<KeyValue<Row>> rows2 = session.findAll(cf, noSqlKeys, true);
		if(keys != null)
			return new CursorTypedResp<T>(meta, keys, rows2);
		else
			return new CursorTypedResp<T>(meta, rows2, indexName);
	}
	
	@Override
	public <T> List<KeyValue<TypedRow<T>>> findAllList(String colFamily, Iterable<T> keys) {
		List<KeyValue<TypedRow<T>>> rows = new ArrayList<KeyValue<TypedRow<T>>>();
		Cursor<KeyValue<TypedRow<T>>> iter = findAll2(colFamily, keys);
		while(iter.next()) {
			KeyValue<TypedRow<T>> keyValue = iter.getCurrent();
			rows.add(keyValue);
		}

		return rows;
	}
	
	@Override
	public void flush() {
		session.flush();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Cursor<KeyValue<TypedRow>> runQuery(String query, Object mgr) {
		//TODO: switch Iterable to Cursor above
		SpiMetaQuery metaQuery = noSqlSessionFactory.parseQueryForAdHoc(query, mgr);
		
		SpiQueryAdapter spiQueryAdapter = metaQuery.createQueryInstanceFromQuery(session); 
		
		AbstractCursor<IndexColumnInfo> iter = spiQueryAdapter.getResultList();
		ViewInfo mainView = metaQuery.getMainViewMeta();
		Iterable<byte[]> indexIterable = new IterableIndex(mainView, iter);

		DboTableMeta meta = mainView.getTableMeta();
		Cursor results = this.findAllImpl2(meta, null, indexIterable, metaQuery.getQuery());
		
		///Hmmmmmm, this is really where we could strip off false positives from the query, create an iterable that
		//skips false positives so as the client loops, we skip some of the results based on that they are false
		//AND we could then trigger index rebuilds as we see there are false columns
		
		
		return results;
	}

	@Override
	public List<KeyValue<TypedRow>> runQueryList(String query, Object noSqlEntityMgr) {
		List<KeyValue<TypedRow>> rows = new ArrayList<KeyValue<TypedRow>>();
		Cursor<KeyValue<TypedRow>> iter = runQuery(query, noSqlEntityMgr);
		while(iter.next()) {
			KeyValue<TypedRow> keyValue = iter.getCurrent();
			rows.add(keyValue);
		}
		return rows;
	}
}