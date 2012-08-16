package com.alvazan.orm.layer0.base;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.base.Partition;
import com.alvazan.orm.api.base.PartitionInfo;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlPartitionByThisField;
import com.alvazan.orm.api.spi3.meta.MetaQuery;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi5.SpiQueryAdapter;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaField;
import com.alvazan.orm.impl.meta.data.MetaInfo;

public class PartitionImpl<T> implements Partition<T> {

	@SuppressWarnings("rawtypes")
	@Inject
	private Provider<QueryAdapter> adapterFactory;
	@Inject
	private MetaInfo metaInfo;
	private MetaClass<T> metaClass;
	private String partitionByColumn;
	private NoSqlSession session;
	private BaseEntityManagerImpl entityMgr;
	private String partitionId;

	@SuppressWarnings("unchecked")
	@Override
	public Query<T> createNamedQuery(String name) {
		MetaQuery<T> metaQuery = metaClass.getNamedQuery(name);
		
		SpiQueryAdapter spiAdapter = metaQuery.createSpiMetaQuery(partitionByColumn, partitionId, session);
		
		//We cannot return MetaQuery since it is used by all QueryAdapters and each QueryAdapter
		//runs in a different thread potentially while MetaQuery is one used by all threads
		QueryAdapter<T> adapter = adapterFactory.get();
		adapter.setup(metaQuery, spiAdapter, entityMgr, metaClass);
		return adapter;
	}

	@Override
	public Query<T> createNamedQueryJoin(String name, PartitionInfo... info) {
		throw new UnsupportedOperationException("We do not support joins just yet");
	}

	public void setup(MetaClass<T> metaClass2, String partitionBy,
			Object partitionObj, BaseEntityManagerImpl entityMgr, NoSqlSession session2) {
		this.metaClass = metaClass2;
		this.partitionByColumn = partitionBy;
		if(partitionObj != null) {
			if(partitionByColumn == null) {
				throw new IllegalArgumentException("partitionBy cannot be null if partitionObj is supplied");
			} else if(!metaClass2.isPartitioned()) {
				throw new IllegalArgumentException("This table="+metaClass2.getMetaClass().getName()+" is not partitioned(no "+NoSqlPartitionByThisField.class
						+" annotation on any field was there, so you can't supply a partitionBy param(use null or call mgr.createNamedQuery instead!!!)");
			}
			MetaField<T> metaCol = metaClass2.getMetaFieldByCol(partitionByColumn);
			if(metaCol == null)
				throw new IllegalArgumentException("The partitionBy argument of="+partitionBy+" is NOT a column on table representing entity="+metaClass2.getMetaClass().getName());
			this.partitionId = metaCol.translateToString(partitionObj);
		}
		
		this.entityMgr = entityMgr;
		this.session = session2;
	}
}
