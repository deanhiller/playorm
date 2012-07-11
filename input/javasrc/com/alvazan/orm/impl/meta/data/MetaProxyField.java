package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;

import com.alvazan.orm.api.base.Converter;
import com.alvazan.orm.api.base.exc.ChildWithNoPkException;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.layer2.MetaTableDbo;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;

public class MetaProxyField<OWNER, PROXY> extends MetaAbstractField<OWNER> {

	//ClassMeta Will eventually have the idField that has the converter!!!
	//once it is scanned
	private MetaClass<PROXY> classMeta;
	
	@Override
	public String toString() {
		return "MetaProxyField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType().getName()+ "), columnName=" + columnName + "]";
	}

	public void translateFromColumn(Column column, OWNER entity, NoSqlSession session) {
		Object proxy = convertIdToProxy(column.getValue(), session);
		ReflectionUtil.putFieldValue(entity, field, proxy);
	}
	
	@SuppressWarnings("unchecked")
	public void translateToColumn(OWNER entity, Column col) {
		PROXY value = (PROXY) ReflectionUtil.fetchFieldValue(entity, field);
		//Value is the Account.java or a Proxy of Account.java field and what we need to save in 
		//the database is the ID inside this Account.java object!!!!
		byte[] byteVal = classMeta.convertProxyToId(value);
		if(byteVal == null && value != null) { 
			//if value is not null byt we get back a byteVal of null, it means the entity has not been
			//initialized with a key yet, BUT this is required to be able to save this object
			String owner = "'"+field.getDeclaringClass().getSimpleName()+"'";
			String child = "'"+field.getType().getSimpleName()+"'";
			String fieldName = "'"+field.getType().getSimpleName()+" "+field.getName()+"'";
			throw new ChildWithNoPkException("The entity you are saving of type="+owner+" has a field="+fieldName
					+" that does not yet have a primary key so you cannot save it.  To correct this\n" +
					"problem, you can either\n"
					+"1. SAVE the "+child+" BEFORE you save the "+owner+" OR\n"
					+"2. Call entityManager.fillInWithKey(Object entity), then SAVE your "+owner+"', then save your "+child+" NOTE that this" +
							"\nmethod #2 is used for when you have a bi-directional relationship where each is a child of the other");
		}
		
		col.setName(columnName);
		col.setValue(byteVal);
	}

	public PROXY convertIdToProxy(byte[] id, NoSqlSession session) {
		return classMeta.convertIdToProxy(id, session);
	}
	
	public void setup(Field field2, String colName, MetaClass<PROXY> classMeta) {
		MetaTableDbo fkToTable = classMeta.getMetaDbo();
		super.setup(field2, colName, fkToTable, null, false);
		this.classMeta = classMeta;
	}

	@Override
	public Object translateToIndexFormat(OWNER entity) {
		String idStr = translateIfEntity(entity);
		return idStr;
	}

	@Override
	public Class<?> getFieldType() {
		return this.field.getType();
	}

	@SuppressWarnings("unchecked")
	@Override
	public String translateIfEntity(Object entity) {
		PROXY value = (PROXY) ReflectionUtil.fetchFieldValue(entity, field);
		MetaIdField<PROXY> idField = classMeta.getIdField();
		Converter converter = idField.getConverter();
		String idStr = converter.convertToIndexFormat(value);
		return idStr;
	}

}
