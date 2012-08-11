package com.alvazan.orm.impl.meta.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javassist.util.proxy.Proxy;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.base.KeyValue;
import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.api.spi3.db.conv.StandardConverters;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;

public class MetaClassInheritance<T> extends MetaAbstractClass<T> {

	@SuppressWarnings("rawtypes")
	@Inject
	private Provider<MetaClassSingle> classMetaProvider;	
	/**
	 * For inheritance for a single table, we have multiple proxies for this class that we may need to
	 * create.
	 */
	private Map<String, MetaClassSingle<T>> dbTypeToMeta = new HashMap<String, MetaClassSingle<T>>();
	private Map<Class, String> classToType = new HashMap<Class, String>();
	private String discriminatorColumnName;
	private byte[] discColAsBytes;
	
	@SuppressWarnings("unchecked")
	public MetaClassSingle<?> findOrCreate(Class<?> clazz, Class<?> parent) {
		NoSqlDiscriminatorColumn col = clazz.getAnnotation(NoSqlDiscriminatorColumn.class);
		if(col == null)
			throw new IllegalArgumentException("Class "+parent.getName()+" in the NoSqlInheritance annotation, specifies a class" +
					" that is missing the NoSqlDiscriminatorColumn annotation.  Class to add annotation to="+clazz.getName());
		else if(!parent.isAssignableFrom(clazz)) 
			throw new IllegalArgumentException("Class "+clazz+" is not a subclass of "+parent+" but the" +
					" NoSqlInheritance annotation specifies that class so it needs to be a subclass");
		
		String columnValue = col.value();
		MetaClassSingle<T> metaSingle = dbTypeToMeta.get(columnValue);
		if(metaSingle != null)
			return metaSingle;
		
		metaSingle = classMetaProvider.get();
		dbTypeToMeta.put(columnValue, metaSingle);
		classToType.put(clazz, columnValue);
		return metaSingle;
	}

	@Override
	public boolean hasIndexedField() {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public KeyValue<T> translateFromRow(Row row, NoSqlSession session) {
		MetaClassSingle<T> metaClassSingle = retrieveMeta(row);
		return metaClassSingle.translateFromRow(row, session);
	}

	private MetaClassSingle<T> retrieveMeta(Row row) {
		Column column = row.getColumn(discColAsBytes);
		
		String type = StandardConverters.convertFromBytes(String.class, column.getValue());
		MetaClassSingle<T> metaClassSingle = this.dbTypeToMeta.get(type);
		return metaClassSingle;
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
	public Tuple<T> convertIdToProxy(Row row, byte[] id, NoSqlSession session,
			CacheLoadCallback cacheLoadCallback) {
		MetaClassSingle<T> metaClassSingle = retrieveMeta(row);
		return metaClassSingle.convertIdToProxy(row, id, session, cacheLoadCallback);
	}

	@Override
	public List<MetaField<T>> getIndexedColumns() {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public void fillInInstance(Row row, NoSqlSession session, T inst) {
		MetaClassSingle<T> metaClassSingle = retrieveMeta(row);
		metaClassSingle.fillInInstance(row, session, inst);
	}

	public void setDiscriminatorColumnName(String discColumn) {
		discriminatorColumnName = discColumn;
		discColAsBytes = StandardConverters.convertToBytes(discColumn);
	}
	
}