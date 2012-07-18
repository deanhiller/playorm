package com.alvazan.orm.api.spi.layer2;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.OneToMany;

@NoSqlEntity
public class MetaTableDbo {

	@Id
	private String columnFamily;
	@OneToMany(entityType=MetaColumnDbo.class, keyFieldForMap="columnName")
	private Map<String, MetaColumnDbo> nameToField = new HashMap<String, MetaColumnDbo>();

	
	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public void addField(MetaColumnDbo fieldDbo) {
		nameToField.put(fieldDbo.getColumnName(), fieldDbo);
	}
	
	public MetaColumnDbo getMetaField(String attributeName) {
		return nameToField.get(attributeName);
	}
	
	@Override
	public String toString() {
		return "[tablename="+columnFamily+" indexedcolumns="+nameToField.values()+"]";
	}

}