package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.api.spi.KeyGenerator;
import com.alvazan.orm.layer2.nosql.Row;

public class MetaIdField {

	protected Field field;
	private String columnName;
	private Converter converter;
	
	private boolean useGenerator;
	private KeyGenerator generator;

	@Override
	public String toString() {
		return "MetaField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ "), columnName=" + columnName + "]";
	}

	public void translateFromRow(Row row, Object entity) {
		byte[] rowKey = row.getKey();
		Object value = convertIdFromNoSql(rowKey);
		ReflectionUtil.putFieldValue(entity, field, value);
	}
	
	public void translateToRow(Object entity, RowToPersist row) {
		Object value = ReflectionUtil.fetchFieldValue(entity, field);
		Object id = fetchFinalId(value);
		byte[] byteVal = convertIdToNoSql(id);
		row.setKey(byteVal);
	}

	private Object fetchFinalId(Object entity) {
		Object id = ReflectionUtil.fetchFieldValue(entity, field);
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

	public void setup(Field field2, String colName, boolean useGenerator, KeyGenerator gen, Converter converter) {
		this.field = field2;
		this.field.setAccessible(true);
		this.useGenerator = useGenerator;
		this.generator = gen;
		this.converter = converter;
	}

	public Object convertIdFromNoSql(byte[] key) {
		return converter.convertFromNoSql(key);
	}
	public byte[] convertIdToNoSql(Object typedKey) {
		return converter.convertToNoSql(typedKey);
	}

}
