package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.api.spi.KeyGenerator;
import com.alvazan.orm.layer2.nosql.Row;

public class MetaIdField {

	protected Field field;
	private String columnName;
	private Converter converter;
	
	private boolean useGenerator;
	private KeyGenerator generator;
	private Method method;

	@Override
	public String toString() {
		return "MetaField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ "), columnName=" + columnName + "]";
	}

	public Object translateFromRow(Row row, Object entity) {
		byte[] rowKey = row.getKey();
		Object entityId = converter.convertFromNoSql(rowKey);
		ReflectionUtil.putFieldValue(entity, field, entityId);
		return entityId;
	}
	
	public void translateToRow(Object entity, RowToPersist row) {
		Object idInEntity = ReflectionUtil.fetchFieldValue(entity, field);
		Object id = fetchFinalId(idInEntity, entity);
		byte[] byteVal = converter.convertToNoSql(id);
		row.setKey(byteVal);
	}

	private Object fetchFinalId(Object idInEntity, Object entity) {
		Object id = idInEntity;
		if(!useGenerator) {
			if(id == null)
				throw new IllegalArgumentException("Entity has @NoSqlEntity(usegenerator=false) but this entity has no id="+entity);
			return id;
		} else if(id != null)
			return id;
		
		Object newId = generator.generateNewKey(entity);
		ReflectionUtil.putFieldValue(entity, field, newId);
		return newId;
	}

	public void setup(Field field2, Method idMethod, String colName, boolean useGenerator, KeyGenerator gen, Converter converter) {
		this.field = field2;
		this.method = idMethod;
		this.field.setAccessible(true);
		this.useGenerator = useGenerator;
		this.generator = gen;
		this.converter = converter;
	}

	public Converter getConverter() {
		return converter;
	}

	public Field getField() {
		return field;
	}

	public Method getIdMethod() {
		return method;
	}

}
