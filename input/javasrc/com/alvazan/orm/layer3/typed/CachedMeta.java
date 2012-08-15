package com.alvazan.orm.layer3.typed;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboColumnToManyMeta;
import com.alvazan.orm.api.spi3.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;

@Singleton
public class CachedMeta {

	private Map<String, DboTableMeta> cachedMeta = new HashMap<String, DboTableMeta>();
	private NoSqlEntityManager mgr;
	
	public void init(NoSqlEntityManagerFactory factory) {
		mgr = factory.createEntityManager();
	}
	
	public DboTableMeta getMeta(String colFamily) {
		DboTableMeta dboTableMeta = cachedMeta.get(colFamily);
		if(dboTableMeta != null)
			return dboTableMeta;
		return loadAllTableData(colFamily);
	}
	
	private DboTableMeta loadAllTableData(String colFamily) {
		//only synchronize on the column family we need to create so we create it once
		synchronized(colFamily.intern()) {
			DboTableMeta dboTableMeta = cachedMeta.get(colFamily);
			if(dboTableMeta != null)
				return dboTableMeta;
			
			DboTableMeta table = mgr.find(DboTableMeta.class, colFamily);
			
			//We don't want lots of threads writing data into this structure as it reads from the database so instead
			//we will prefetch everything that is typically used here....
			table.getIdColumnMeta().getColumnName(); //load this id stuff
			//load all columns as well
			for(DboColumnMeta col : table.getAllColumns()) {
				if(col instanceof DboColumnToManyMeta) {
					((DboColumnToManyMeta)col).getFkToColumnFamily().getIdColumnMeta().getColumnName();
				} else if(col instanceof DboColumnToOneMeta) {
					((DboColumnToOneMeta)col).getFkToColumnFamily().getIdColumnMeta().getColumnName();
				}
			}
	
			cachedMeta.put(colFamily, table);
			return table;
		}
	}
}
