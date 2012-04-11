package com.alvazan.orm.impl.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alvazan.orm.layer2.nosql.Row;
import com.alvazan.orm.layer3.spi.Column;

public class MetaClass<T> {

	private Class<T> metaClass;
	private String columnFamily;
	
	private MetaIdField idField;
	private List<MetaField> fields = new ArrayList<MetaField>();
	
	void setMetaClass(Class<T> clazz) {
		this.metaClass = clazz;
	}

	Class<T> getMetaClass() {
		return metaClass;
	}

	public T translateFromRow(Row row) {
		T inst = ReflectionUtil.create(metaClass);
		String key = row.getKey();
		idField.fillInId(inst, key);

		Map<String, Column> columns = row.getColumns();
		for(MetaField field : fields) {
			field.translateFromColumn(columns, inst);
		}
		return inst;
	}
	
	public RowToPersist translateToRow(Object entity) {
		String id = idField.fillInOrCheckForId(entity);
		RowToPersist row = new RowToPersist();
		row.setKey(id);
		
		for(MetaField m : fields) {
			Column col = m.translateToColumn(entity);
			row.getColumns().add(col);
		}
		
		return row;
	}

	public String getColumnFamily() {
		return columnFamily;
	}
	void setColumnFamily(String colFamily) {
		this.columnFamily = colFamily;
	}

	void addMetaField(MetaField field) {
		fields.add(field);
	}

	void setIdField(MetaIdField field) {
		this.idField = field;
	}
}
