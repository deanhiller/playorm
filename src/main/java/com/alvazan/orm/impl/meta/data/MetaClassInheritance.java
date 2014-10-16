package com.alvazan.orm.impl.meta.data;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javassist.util.proxy.Proxy;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;

@SuppressWarnings("rawtypes")
public class MetaClassInheritance<T> extends MetaAbstractClass<T> {

	@Inject
	private Provider<MetaClassSingle> classMetaProvider;	
	/**
	 * For inheritance for a single table, we have multiple proxies for this class that we may need to
	 * create.
	 */
	private Map<String, MetaClassSingle<T>> dbTypeToMeta = new HashMap<String, MetaClassSingle<T>>();
	private Map<String, String> classToType = new HashMap<String, String>();
	private String discriminatorColumnName;
	private byte[] discColAsBytes;
	
	public String getDiscriminatorColumnName() {
		return discriminatorColumnName;
	}
	
	public Collection<MetaClassSingle<T>> fetchSubclassList() {
		return dbTypeToMeta.values();
	}
	
	public SpiMetaQuery getNamedQuery(Class<? extends T> clazz, String name) {
		if(clazz.equals(getMetaClass()))
			return super.getNamedQuery(clazz, name);
		
		String type = classToType.get(clazz.getName());
		MetaClassSingle<T> metaSingle = dbTypeToMeta.get(type);
		return metaSingle.getNamedQuery(clazz, name);
	}
	
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
		//All the subclasses need to share the same meta Dbo object!!!! as it is one table for the
		//whole class heirarchy
		metaSingle.setSharedMetaDbo(getMetaDbo());
		dbTypeToMeta.put(columnValue, metaSingle);
		classToType.put(clazz.getName(), columnValue);
		return metaSingle;
	}

	@Override
	public boolean hasIndexedField(T entity) {
		Class clazz = entity.getClass();
		if(entity instanceof Proxy) {
			clazz = entity.getClass().getSuperclass();
		}
		String type = classToType.get(clazz.getName());
		MetaClassSingle<T> metaClassSingle = dbTypeToMeta.get(type);
		return metaClassSingle.hasIndexedField(entity);
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


	@Override
	public RowToPersist translateToRow(T entity) {
		Class clazz = entity.getClass();
		if(entity instanceof Proxy) {
			clazz = entity.getClass().getSuperclass();
		}
		String type = classToType.get(clazz.getName());
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
		Class clazz = proxy.getClass().getSuperclass();
		String type = classToType.get(clazz.getName());
		MetaClassSingle<T> metaClassSingle = dbTypeToMeta.get(type);
		return metaClassSingle.findIndexRemoves(proxy, rowKey);
	}

	@Override
	public MetaField<T> getMetaFieldByCol(Class c, String columnName) {
		String type = classToType.get(c.getName());
		MetaClassSingle<T> metaSingle = dbTypeToMeta.get(type);
		return metaSingle.getMetaFieldByCol(c, columnName);
	}

	@Override
	public Class<? extends T> getProxyClass(Class<?> clazz) {
		String type = classToType.get(clazz.getName());
		MetaClassSingle<T> metaSingle = dbTypeToMeta.get(type);
		return metaSingle.getProxyClass(clazz);
	}

	@Override
	public Tuple<T> convertIdToProxy(Row row, NoSqlSession session, byte[] nonVirtKey,
			CacheLoadCallback cacheLoadCallback) {
		if(row == null)
			throw new IllegalArgumentException("bug, inheritance does not support who called this so you can't have inheritance along with it, we need to fix so let us know");
		MetaClassSingle<T> metaClassSingle = retrieveMeta(row);
		return metaClassSingle.convertIdToProxy(row, session, nonVirtKey, cacheLoadCallback);
	}

	@Override
	public List<MetaField<T>> getIndexedColumns() {
		throw new UnsupportedOperationException("not done yet.  ie. don't have index support for inheritance tables yet");
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

	@Override
	public boolean isPartitioned() {
		throw new UnsupportedOperationException("not done yet.  ie. don't have partition support for inheritance tables yet");
	}
	
}