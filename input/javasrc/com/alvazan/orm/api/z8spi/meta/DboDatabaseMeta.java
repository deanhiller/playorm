package com.alvazan.orm.api.z8spi.meta;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;

import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;

@Singleton
@NoSqlEntity
public class DboDatabaseMeta {
	
	/**
	 * This is the key for the SINGLE row of DboDatabaseMeta that exists in the nosql store.
	 */
	public static final String META_DB_ROWKEY = "nosqlorm";
	
	@NoSqlId(usegenerator=false)
	private String id = META_DB_ROWKEY;
	
	@NoSqlOneToMany(keyFieldForMap="columnFamily")
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
