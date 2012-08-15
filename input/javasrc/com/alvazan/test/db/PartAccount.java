package com.alvazan.test.db;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.base.Partition;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;

@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findBetween", query="select b from Account b where b.users >= :begin and b.users < :end"),
	@NoSqlQuery(name="findAll", query="select *  from Account d"),
	@NoSqlQuery(name="findAnd", query="select *  FROM Account a WHERE a.name=:name and a.isActive=:active"),
	@NoSqlQuery(name="findOr", query="select *  FROM Account a WHERE a.name=:name or a.isActive=:active")
})
public class PartAccount extends AccountSuper{

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

	public static List<PartAccount> findBetween(Partition<PartAccount> index, float begin, float to) {
		Query<PartAccount> query = index.getNamedQuery("findBetween");
		query.setParameter("begin", begin);
		query.setParameter("end", to);
		return query.getResultList();
	}
	public static List<PartAccount> findAll(Partition<PartAccount> index) {
		Query<PartAccount> query = index.getNamedQuery("findAll");
		return query.getResultList();
	}
	public static List<PartAccount> findAnd(Partition<PartAccount> index, String name, Boolean active) {
		Query<PartAccount> query = index.getNamedQuery("findAnd");
		query.setParameter("name", name);
		query.setParameter("active", active);
		return query.getResultList();
	}

	public static List<PartAccount> findOr(Partition<PartAccount> index, String name,
			boolean active) {
		Query<PartAccount> query = index.getNamedQuery("findOr");
		query.setParameter("name", name);
		query.setParameter("active", active);
		return query.getResultList();
	}

	public List<PartitionedTrade> getActivities() {
		return activities;
	}

	public void addActivity(PartitionedTrade act1) {
		activities.add(act1);
	}
	
}
