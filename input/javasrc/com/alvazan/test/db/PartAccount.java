package com.alvazan.test.db;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.spi3.KeyValue;

@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findAll", query="select *  from TABLE d")
})
public class PartAccount {

	@NoSqlId
	private String id;
	
	@NoSqlIndexed
	private String businessName;
	
	private int someNumber;

	@NoSqlOneToMany(entityType=PartitionedTrade.class)
	private List<PartitionedTrade> activities = new ArrayList<PartitionedTrade>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getSomeNumber() {
		return someNumber;
	}

	public void setSomeNumber(int someNumber) {
		this.someNumber = someNumber;
	}

	public String getBusinessName() {
		return businessName;
	}

	public void setBusinessName(String businessName) {
		this.businessName = businessName;
	}

	public static List<KeyValue<PartAccount>> findAll2(NoSqlEntityManager mgr) {
		Query<PartAccount> query = mgr.createNamedQuery(PartAccount.class, "findAll");
		return query.getResultKeyValueList();		
	}
	public static List<PartAccount> findAll(NoSqlEntityManager mgr) {
		Query<PartAccount> query = mgr.createNamedQuery(PartAccount.class, "findAll");
		return query.getResultList();
	}

	public List<PartitionedTrade> getActivities() {
		return activities;
	}

	public void addActivity(PartitionedTrade act1) {
		activities.add(act1);
	}

}
