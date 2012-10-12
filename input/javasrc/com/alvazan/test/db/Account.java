package com.alvazan.test.db;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.base.CursorToMany;
import com.alvazan.orm.api.base.CursorToManyImpl;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlManyToMany;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;

@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findBetween", query="select b from Account as b where b.users >= :begin and (b.name = :name or b.name = :name) and b.users < :end"),
	@NoSqlQuery(name="findAll", query="select *  from Account as d"),
	@NoSqlQuery(name="findAnd", query="select *  FROM Account as a WHERE a.name=:name and a.isActive=:active"),
	@NoSqlQuery(name="findOr", query="select *  FROM Account as a WHERE a.name=:name or a.isActive=:active")
})
public class Account extends AccountSuper{

	@NoSqlId
	private String id;
	
	@NoSqlIndexed
	private String name;
	
	@NoSqlIndexed
	private Float users;

	//@Transient
	@NoSqlOneToMany
	private List<Activity> activities = new ArrayList<Activity>();
	
	@NoSqlManyToMany
	private CursorToMany<Activity> activitiesCursor = new CursorToManyImpl<Activity>();
	
	public Account() {}
	
	public Account(String id ) {
		this.id = id;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Float getUsers() {
		return users;
	}

	public void setUsers(Float users) {
		this.users = users;
	}
	
	public static List<Account> findBetween(NoSqlEntityManager mgr, float begin, float to) {
		Query<Account> query = mgr.createNamedQuery(Account.class, "findBetween");
		query.setParameter("begin", begin);
		query.setParameter("end", to);
		return query.getResultList(0, null);
	}
	public static List<Account> findAll(NoSqlEntityManager mgr) {
		Query<Account> query = mgr.createNamedQuery(Account.class, "findAll");
		return query.getResultList(0, null);
	}
	public static List<Account> findAnd(NoSqlEntityManager mgr, String name, Boolean active) {
		Query<Account> query = mgr.createNamedQuery(Account.class, "findAnd");
		query.setParameter("name", name);
		query.setParameter("active", active);
		return query.getResultList(0, null);
	}

	public static List<Account> findOr(NoSqlEntityManager mgr, String name,
			boolean active) {
		Query<Account> query = mgr.createNamedQuery(Account.class, "findOr");
		query.setParameter("name", name);
		query.setParameter("active", active);
		return query.getResultList(0, null);
	}

	public List<Activity> getActivities() {
		return activities;
	}

	
	public CursorToMany<Activity> getActivitiesCursor() {
		return activitiesCursor;
	}

	public void addActivity(Activity act1) {
		activities.add(act1);
		activitiesCursor.addElement(act1);
	}
	
}
