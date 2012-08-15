package com.alvazan.test.db;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.base.Index;
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
public class PartitionAccount extends AccountSuper{

	@NoSqlId
	private String id;
	
	@NoSqlIndexed
	private String businessName;
	
	private int someNumber;

	@NoSqlOneToMany(entityType=PartitionTrade.class)
	private List<PartitionTrade> activities = new ArrayList<PartitionTrade>();

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

	public static List<PartitionAccount> findBetween(Index<PartitionAccount> index, float begin, float to) {
		Query<PartitionAccount> query = index.getNamedQuery("findBetween");
		query.setParameter("begin", begin);
		query.setParameter("end", to);
		return query.getResultList();
	}
	public static List<PartitionAccount> findAll(Index<PartitionAccount> index) {
		Query<PartitionAccount> query = index.getNamedQuery("findAll");
		return query.getResultList();
	}
	public static List<PartitionAccount> findAnd(Index<PartitionAccount> index, String name, Boolean active) {
		Query<PartitionAccount> query = index.getNamedQuery("findAnd");
		query.setParameter("name", name);
		query.setParameter("active", active);
		return query.getResultList();
	}

	public static List<PartitionAccount> findOr(Index<PartitionAccount> index, String name,
			boolean active) {
		Query<PartitionAccount> query = index.getNamedQuery("findOr");
		query.setParameter("name", name);
		query.setParameter("active", active);
		return query.getResultList();
	}

	public List<PartitionTrade> getActivities() {
		return activities;
	}

	public void addActivity(PartitionTrade act1) {
		activities.add(act1);
	}
	
}
