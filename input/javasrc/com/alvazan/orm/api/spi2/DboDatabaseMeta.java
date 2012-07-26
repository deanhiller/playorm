package com.alvazan.orm.api.spi2;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.OneToMany;

@NoSqlEntity()
public class DboDatabaseMeta {

	@Id(usegenerator=false)
	private String id;
	
	@OneToMany(entityType=DboTableMeta.class, keyFieldForMap="columnFamily")
	private Map<String, DboTableMeta> colFamilyToMeta = new HashMap<String, DboTableMeta>();
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void addMetaClassDbo(DboTableMeta metaClass) {
		colFamilyToMeta.put(metaClass.getColumnFamily(), metaClass);
	}
	
	public DboTableMeta getMeta(String tableName) {
		return colFamilyToMeta.get(tableName);
	}

	public Collection<DboTableMeta> getAllTables() {
		return colFamilyToMeta.values();
	}
}
