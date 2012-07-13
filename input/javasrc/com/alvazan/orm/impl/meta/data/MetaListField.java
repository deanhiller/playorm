package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.base.exc.ChildWithNoPkException;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.layer2.MetaTableDbo;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.collections.ListProxyFetchAll;
import com.alvazan.orm.impl.meta.data.collections.MapProxyFetchAll;

public class MetaListField<OWNER, PROXY> extends MetaAbstractField<OWNER> {

	private MetaClass<PROXY> classMeta;
	private Field fieldForKey;
	private static final byte DEFAULT_DELIMETER = (byte) ',';
	
	@Override
	public void translateFromColumn(Column column, OWNER entity,
			NoSqlSession session) {
		Object proxy;
		if(field.getType().equals(Map.class))
			proxy = translateFromColumnMap(column, entity, session);
		else
			proxy = translateFromColumnList(column, entity, session);
		
		ReflectionUtil.putFieldValue(entity, field, proxy);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Map translateFromColumnMap(Column column,
			OWNER entity, NoSqlSession session) {
		List<byte[]> keys = parseOutKeyList(column);
		MapProxyFetchAll proxy = new MapProxyFetchAll(session, classMeta, keys, fieldForKey);
		return proxy;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<PROXY> translateFromColumnList(Column column,
			OWNER entity, NoSqlSession session) {
		List<byte[]> keys = parseOutKeyList(column);
		List<PROXY> retVal = new ListProxyFetchAll(session, classMeta, keys);
		return retVal;
	}

	private List<byte[]> parseOutKeyList(Column column) {
		byte[] data = column.getValue();
		List<byte[]> entities = new ArrayList<byte[]>();
		List<Byte> currentList = new ArrayList<Byte>();
		for(byte b : data) {
			if(b == DEFAULT_DELIMETER) {
				byte[] key = toByteArray(currentList);
				entities.add(key);
				currentList = new ArrayList<Byte>();
				
			} else
				currentList.add(b);
		}
		
		return entities;
	}

	private byte[] toByteArray(List<Byte> currentList) {
		byte[] data = new byte[currentList.size()];
		for(int i = 0; i < currentList.size(); i++) {
			data[i] = currentList.get(i).byteValue();
		}
		return data;
	}

	@Override
	public void translateToColumn(OWNER entity, Column col) {
		if(field.getType().equals(Map.class))
			translateToColumnMap(entity, col);
		else
			translateToColumnList(entity, col);
	}

	@SuppressWarnings("unchecked")
	private void translateToColumnList(OWNER entity, Column col) {
		List<PROXY> values = (List<PROXY>) ReflectionUtil.fetchFieldValue(entity, field);
		translateToColumnImpl(values, col);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void translateToColumnMap(OWNER entity, Column col) {
		Map mapOfProxies = (Map) ReflectionUtil.fetchFieldValue(entity, field);
		Collection collection = mapOfProxies.values();
		translateToColumnImpl(collection, col);
	}

	private void translateToColumnImpl(Collection<PROXY> collection, Column col) {
		col.setName(columnName);
		List<Byte> data = new ArrayList<Byte>();
		
		for(PROXY proxy : collection) {
			byte[] bytes = translateOne(proxy);
			for(byte b : bytes) {
				data.add(b);
			}
			data.add(DEFAULT_DELIMETER);
		}

		byte[] finalVal = new byte[data.size()];
		for(int i = 0; i < data.size(); i++) {
			finalVal[i] = data.get(i);
		}
		col.setValue(finalVal);
	}

	private byte[] translateOne(PROXY proxy) {
		//Value is the Account.java or a Proxy of Account.java field and what we need to save in 
		//the database is the ID inside this Account.java object!!!!
		byte[] byteVal = classMeta.convertProxyToId(proxy);
		if(byteVal == null) {
			String owner = "'"+field.getDeclaringClass().getSimpleName()+"'";
			String child = "'"+classMeta.getMetaClass().getSimpleName()+"'";
			String fieldName = "'"+field.getType().getSimpleName()+" "+field.getName()+"'";
			throw new ChildWithNoPkException("The entity you are saving of type="+owner+" has a field="+fieldName
					+" which has an entity in the collection that does not yet have a primary key so you cannot save it. \n" +
					"The offending object is="+proxy+"   To correct this\n" +
					"problem, you can either\n"
					+"1. SAVE the "+child+" BEFORE you save the "+owner+" OR\n"
					+"2. Call entityManager.fillInWithKey(Object entity), then SAVE your "+owner+"', then save your "+child+" NOTE that this" +
							"\nmethod #2 is used for when you have a bi-directional relationship where each is a child of the other");
		}
		return byteVal;
	}

	@Override
	public Class<?> getFieldType() {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public String translateIfEntity(Object value) {
		throw new UnsupportedOperationException("not done yet");
	}

	@Override
	public Object translateToIndexFormat(OWNER entity) {
		throw new UnsupportedOperationException("This field cannot be indexed");
	}

	public void setup(Field field, String colName, MetaClass<PROXY> classMeta, Field fieldForKey) {
		MetaTableDbo fkToTable = classMeta.getMetaDbo();
		super.setup(field, colName, fkToTable, null, true);
		this.classMeta = classMeta;
		this.fieldForKey = fieldForKey;
	}

	@Override
	public String toString() {
		return "MetaListField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType().getName()
				+ "<"+classMeta.getMetaClass().getName()+">), columnName=" + columnName + "]";
	}
}
