package com.alvazan.orm.impl.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.KeyValue;
import com.alvazan.orm.layer2.nosql.Row;
import com.alvazan.orm.layer3.spi.Column;

public class MetaClass<T> {

	private Class<T> metaClass;
	private String columnFamily;
	
	private MetaIdField idField;
	private List<MetaField> fields = new ArrayList<MetaField>();
	
	
	@Override
	public String toString() {
		return "MetaClass [metaClass=" + metaClass + ", columnFamily="
				+ columnFamily + "]";
	}

	void setMetaClass(Class<T> clazz) {
		this.metaClass = clazz;
	}

	Class<T> getMetaClass() {
		return metaClass;
	}

	public KeyValue<T> translateFromRow(Row row) {
		T inst = ReflectionUtil.create(metaClass);
		Object key = idField.translateFromRow(row, inst);

		Map<String, Column> columns = row.getColumns();
		for(MetaField field : fields) {
			field.translateFromColumn(columns, inst);
		}
		
		KeyValue<T> keyVal = new KeyValue<T>();
		keyVal.setKey(key);
		keyVal.setValue(inst);
		return keyVal;
	}
	
	public RowToPersist translateToRow(Object entity) {
		RowToPersist row = new RowToPersist();
		idField.translateToRow(entity, row);
		
		for(MetaField m : fields) {
			Column col = new Column();
			m.translateToColumn(entity, col);
			row.getColumns().add(col);
		}
		
		return row;
	}

	public String getColumnFamily() {
		return columnFamily;
	}
	void setColumnFamily(String colFamily) {
		if(colFamily == null)
			throw new IllegalArgumentException("colFamily cannot be null");
		this.columnFamily = colFamily;
	}

	void addMetaField(MetaField field) {
		if(field == null)
			throw new IllegalArgumentException("field cannot be null");
		fields.add(field);
	}

	void setIdField(MetaIdField field) {
		if(field == null)
			throw new IllegalArgumentException("field cannot be null");
		this.idField = field;
	}

	public Object convertIdToProxy(byte[] id) {
		return idField.convertIdToProxy(id);
	}

	public byte[] convertProxyToId(Object value) {
		return idField.convertProxyToId(value);
	}

	public byte[] convertIdToNoSql(Object entityId) {
		return idField.convertIdToNoSql(entityId);
	}
}
