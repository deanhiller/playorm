package com.alvazan.test;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.spi3.meta.DboColumnCommonMeta;
import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.layer5.nosql.cache.MetaFacade;

public class MockFacade implements MetaFacade {

	private Map<String, DboTableMeta> nameToTable = new HashMap<String, DboTableMeta>();
	
	@Override
	public DboTableMeta getColumnFamily(String tableName) {
		DboTableMeta existing = nameToTable.get(tableName);
		if(existing == null) {
			existing = new DboTableMeta();
			existing.setColumnFamily(tableName);
			nameToTable.put(tableName, existing);
			
		}
		return existing;
	}

	@Override
	public DboColumnMeta getColumnMeta(DboTableMeta metaClass, String columnName) {
		DboColumnMeta colMeta = metaClass.getColumnMeta(columnName);
		if(colMeta == null) {
			DboColumnCommonMeta temp = new DboColumnCommonMeta();
			temp.setup(metaClass, columnName, null, true, false);
			metaClass.addColumnMeta(temp);
			colMeta = temp;
		}
			
		return colMeta;
	}

	
}
