package com.alvazan.test.db;

import java.util.List;

import com.alvazan.orm.api.Index;
import com.alvazan.orm.api.Query;
import com.alvazan.orm.api.anno.Id;
import com.alvazan.orm.api.anno.Indexed;
import com.alvazan.orm.api.anno.NoSqlEntity;
import com.alvazan.orm.api.anno.NoSqlQueries;
import com.alvazan.orm.api.anno.NoSqlQuery;

@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findBetween", query="select ENTITY b FROM TABLE WHERE b.users >= :from and b.users < :to"),
	@NoSqlQuery(name="findAll", query="select ENTITY d FROM TABLE"),
	@NoSqlQuery(name="findAnd", query="select ENTITY a FROM TABLE WHERE a.name=:name and a.isActive=:active"),
	@NoSqlQuery(name="findOr", query="select ENTITY a FROM TABLE WHERE a.name=:name or a.isActive=:active")
})
public class Account extends AccountSuper{

	@Id
	private String id;
	
	@Indexed
	private String name;
	
	@Indexed
	private Float indexedValue;

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

	public Float getIndexedValue() {
		return indexedValue;
	}

	public void setIndexedValue(Float users) {
		this.indexedValue = users;
	}
	
	public static List<Account> findBetween(Index<Account> index, float from, float to) {
		Query<Account> query = index.getNamedQuery("findBetween");
		query.setParameter("from", from);
		query.setParameter("to", to);
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
}
