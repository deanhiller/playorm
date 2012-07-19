package com.alvazan.orm.api.spi.layer2;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.OneToMany;

@NoSqlEntity
public class DboTableMeta {

	@Id(usegenerator=false)
	private String columnFamily;
	
	@OneToMany(entityType=DboColumnMeta.class, keyFieldForMap="columnName")
	private Map<String, DboColumnMeta> nameToField = new HashMap<String, DboColumnMeta>();
	
	private String idColumnType;
	//There is no name stored in the nosql store!!! BUT the queries will use
	//this name to query the data like e.id = :id and when that happens, we look up by primary key
	private String idColumnName;
	
	@SuppressWarnings("rawtypes")
	public void setup(String columnFamily, Class idColumnType) {
		if(columnFamily == null || idColumnType == null)
			throw new IllegalArgumentException("no parameters can be null and one was null.  cf="+columnFamily+" idcolType="+idColumnType);
		this.columnFamily = columnFamily;

	}
	
	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	@SuppressWarnings("rawtypes")
	public void setRowKeyTypeAndName(Class clazz, String name) {
		if(clazz == null)
			throw new IllegalArgumentException("Can only set the row key type to a valid type");
		Class newType = DboColumnMeta.translateType(clazz);
		idColumnType = newType.getName();
		this.idColumnName = name;
	}
	
	public String getIdColumnName() {
		return idColumnName;
	}
	
	@SuppressWarnings("rawtypes")
	public Class getIdType() {
		return DboColumnMeta.classForName(idColumnType);
	}
	
	public Map<String, DboColumnMeta> getColumns() {
		return nameToField;
	}
	
	public void addColumnMeta(DboColumnMeta fieldDbo) {
		nameToField.put(fieldDbo.getColumnName(), fieldDbo);
	}
	
	public DboColumnMeta getColumnMeta(String columnName) {
		return nameToField.get(columnName);
	}
	
	@Override
	public String toString() {
		return "[tablename="+columnFamily+" indexedcolumns="+nameToField.values()+"]";
	}

}