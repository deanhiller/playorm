package com.alvazan.orm.impl.meta.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javassist.util.proxy.Proxy;

import com.alvazan.orm.api.base.KeyValue;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.api.spi3.db.conv.StandardConverters;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;

public class MetaClassInheritance<T> extends MetaAbstractClass<T> {

	/**
	 * For inheritance for a single table, we have multiple proxies for this class that we may need to
	 * create.
	 */
	private Map<String, MetaClassSingle<T>> dbTypeToMeta = new HashMap<String, MetaClassSingle<T>>();
	private Map<Class, String> classToType = new HashMap<Class, String>();
	private String discriminatorColumnName;
	private byte[] discColAsBytes;
	
	public void addProxy(String columnValue, MetaClassSingle<T> metaSingle) {
		MetaClassSingle<T> existing = dbTypeToMeta.get(columnValue);
		if(existing != null)
			throw new IllegalArgumentException("You are doing inheritance strategy single table but two of your" +
					" classes use the same column value="+columnValue+" from enttity="+getMetaClass());
		dbTypeToMeta.put(columnValue, metaSingle);
		classToType.put(metaSingle.getMetaClass(), columnValue);
	}

	@Override
	public boolean hasIndexedField() {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public KeyValue<T> translateFromRow(Row row, NoSqlSession session) {
		Column column = row.getColumn(discColAsBytes);
		String type = StandardConverters.convertFromBytes(String.class, column.getValue());
		MetaClassSingle<T> metaClassSingle = this.dbTypeToMeta.get(type);
		return metaClassSingle.translateFromRow(row, session);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public RowToPersist translateToRow(T entity) {
		Class clazz = entity.getClass();
		if(entity instanceof Proxy) {
			clazz = entity.getClass().getSuperclass();
		}
		String type = classToType.get(clazz);
		MetaClassSingle<T> metaClassSingle = dbTypeToMeta.get(type);
		RowToPersist translateToRow = metaClassSingle.translateToRow(entity);
		
		byte[] value = StandardConverters.convertToBytes(type);
		Column typeCol = new Column();
		typeCol.setName(discColAsBytes);
		typeCol.setValue(value);
		translateToRow.getColumns().add(typeCol);
		return translateToRow;
	}

	@Override
	public List<IndexData> findIndexRemoves(NoSqlProxy proxy, byte[] rowKey) {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public MetaField<T> getMetaFieldByCol(String columnName) {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public Class<? extends T> getProxyClass() {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public Tuple<T> convertIdToProxy(byte[] id, NoSqlSession session,
			CacheLoadCallback cacheLoadCallback) {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public List<MetaField<T>> getIndexedColumns() {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public void fillInInstance(Row row, NoSqlSession session, T inst) {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public void addMetaField(MetaField<T> metaField, boolean isIndexed) {
		throw new UnsupportedOperationException("not done yet");
	}

	public void setDiscriminatorColumnName(String discColumn) {
		discriminatorColumnName = discColumn;
		discColAsBytes = StandardConverters.convertToBytes(discColumn);
	}
	
}