package com.alvazan.test.db;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.Indexed;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.base.anno.OneToMany;

@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findBetween", query="select b from Account b where b.users >= :begin and b.users < :end"),
	@NoSqlQuery(name="findAll", query="select *  from Account d"),
	@NoSqlQuery(name="findAnd", query="select *  FROM Account a WHERE a.name=:name and a.isActive=:active"),
	@NoSqlQuery(name="findOr", query="select *  FROM Account a WHERE a.name=:name or a.isActive=:active")
})
public class Account extends AccountSuper{

	@Id
	private String id;
	
	@Indexed
	private String name;
	
	@Indexed
	private Float users;

	//@Transient
	@OneToMany(entityType=Activity.class)
	private List<Activity> activities = new ArrayList<Activity>();

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
	
	public static List<Account> findBetween(Index<Account> index, float begin, float to) {
		Query<Account> query = index.getNamedQuery("findBetween");
		query.setParameter("begin", begin);
		query.setParameter("end", to);
		return query.getResultList();
	}
	public static List<Account> findAll(Index<Account> index) {
		Query<Account> query = index.getNamedQuery("findAll");
		return query.getResultList();
	}
	public static List<Account> findAnd(Index<Account> index, String name, Boolean active) {
		Query<Account> query = index.getNamedQuery("findAnd");
		query.setParameter("name", name);
		query.setParameter("active", active);
		return query.getResultList();
	}

	public static List<Account> findOr(Index<Account> index, String name,
			boolean active) {
		Query<Account> query = index.getNamedQuery("findOr");
		query.setParameter("name", name);
		query.setParameter("active", active);
		return query.getResultList();
	}

	public List<Activity> getActivities() {
		return activities;
	}

	public void addActivity(Activity act1) {
		activities.add(act1);
	}
	
}
