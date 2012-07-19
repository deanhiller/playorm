package com.alvazan.orm.api.spi.layer2;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.OneToMany;
import com.alvazan.orm.api.base.anno.OneToOne;

@NoSqlEntity
public class DboTableMeta {

	@Id(usegenerator=false)
	private String columnFamily;
	
	@OneToMany(entityType=DboColumnMeta.class, keyFieldForMap="columnName")
	private Map<String, DboColumnMeta> nameToField = new HashMap<String, DboColumnMeta>();
	@OneToOne
	private DboColumnMeta idColumn;
	
	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}
	
	public void setRowKeyMeta(DboColumnMeta idMeta) {
		this.idColumn = idMeta;
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
		return "[tablename="+columnFamily+" indexedcolumns="+nameToField.values()+" pk="+idColumn+"]";
	}

	public DboColumnMeta getIdColumnMeta() {
		return idColumn;
	}

}