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

public class MetaListField<OWNER, PROXY> extends MetaAbstractField<OWNER> {

	private MetaClass<?> classMeta;
	
	@Override
	public void translateFromColumn(Column column, OWNER entity,
			NoSqlSession session) {
		Object proxy;// = convertIdToProxy(column.getValue(), session);
		
		if(field.getType().equals(Map.class))
			proxy = translateFromColumnMap(column, entity, session);
		else
			proxy = translateFromColumnList(column, entity, session);
		
		ReflectionUtil.putFieldValue(entity, field, proxy);
	}

	private List translateFromColumnList(Column column,
			OWNER entity, NoSqlSession session) {
		
		return null;
	}

	private Map translateFromColumnMap(Column column,
			OWNER entity, NoSqlSession session) {
		
		return null;
	}

	@Override
	public void translateToColumn(OWNER entity, Column col) {
		if(field.getType().equals(Map.class))
			translateMap(entity, col);
		else
			translateList(entity, col);
	}

	@SuppressWarnings("unchecked")
	private void translateList(OWNER entity, Column col) {
		List<PROXY> values = (List<PROXY>) ReflectionUtil.fetchFieldValue(entity, field);
		translate(values, col);		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void translateMap(OWNER entity, Column col) {
		Map mapOfProxies = (Map) ReflectionUtil.fetchFieldValue(entity, field);
		Collection collection = mapOfProxies.values();
		translate(collection, col);
	}

	private void translate(Collection<PROXY> collection, Column col) {
		col.setName(columnName);
		byte delimeter = (byte) ',';
		List<Byte> data = new ArrayList<Byte>();
		
		for(PROXY proxy : collection) {
			byte[] bytes = translateOne(proxy);
			for(byte b : bytes) {
				data.add(b);
			}
			data.add(delimeter);
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
		byte[] byteVal = MetaProxyField.convertProxyToId(classMeta, proxy);
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
	public String translateToIndexFormat(OWNER entity) {
		throw new UnsupportedOperationException("not done yet");
	}

	public void setup(Field field, String colName, MetaClass<?> classMeta) {
		MetaTableDbo fkToTable = classMeta.getMetaDbo();
		super.setup(field, colName, fkToTable, null, true);
		this.classMeta = classMeta;
	}

	@Override
	public String toString() {
		return "MetaListField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType().getName()
				+ "<"+classMeta.getMetaClass().getName()+">), columnName=" + columnName + "]";
	}
}
