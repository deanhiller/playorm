package com.alvazan.orm.impl.meta.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.base.KeyValue;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;

public class MetaClassInheritance<T> extends MetaAbstractClass<T> {

	/**
	 * For inheritance for a single table, we have multiple proxies for this class that we may need to
	 * create.
	 */
	private Map<String, Class<? extends T>> proxyClassList;
	
	public void addProxy(String columnValue, Class<? extends T> proxyClass2) {
		if(proxyClassList == null)
			proxyClassList = new HashMap<String, Class<? extends T>>();
		Class<? extends T> existing = proxyClassList.get(columnValue);
		if(existing != null)
			throw new IllegalArgumentException("You are doing inheritance strategy single table but two of your" +
					" classes use the same column value="+columnValue+" from enttity="+getMetaClass());
		proxyClassList.put(columnValue, proxyClass2);
	}

	@Override
	public boolean hasIndexedField() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public KeyValue<T> translateFromRow(Row row, NoSqlSession session) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowToPersist translateToRow(T entity) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<IndexData> findIndexRemoves(NoSqlProxy proxy, byte[] rowKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MetaField<T> getMetaFieldByCol(String columnName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends T> getProxyClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Tuple<T> convertIdToProxy(byte[] id, NoSqlSession session,
			CacheLoadCallback cacheLoadCallback) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<MetaField<T>> getIndexedColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fillInInstance(Row row, NoSqlSession session, T inst) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addMetaField(MetaField<T> metaField, boolean isIndexed) {
		// TODO Auto-generated method stub
		
	}
	
}
