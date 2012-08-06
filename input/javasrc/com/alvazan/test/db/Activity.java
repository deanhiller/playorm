package com.alvazan.test.db;

import java.util.List;

import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;


@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findBetween", query="select *  FROM TABLE e WHERE e.numTimes >= :begin and e.numTimes < :to"),
	@NoSqlQuery(name="findUnique", query="select *  FROM TABLE e WHERE e.uniqueColumn = :unique"),
	@NoSqlQuery(name="findWithParens", query="select * FROM TABLE e WHERE" +
			" e.name=:name and (e.numTimes = :numTimes or e.myFloat = :myFloat)"),	
	@NoSqlQuery(name="findWithoutParens", query="select * FROM TABLE e WHERE" +
			" e.name=:name and e.numTimes = :numTimes or e.myFloat = :myFloat"),
	@NoSqlQuery(name="findWithAnd", query="select * FROM TABLE e WHERE e.name=:name and e.numTimes = :numTimes"),
	@NoSqlQuery(name="findWithOr", query="select * FROM TABLE e WHERE e.name=:name or e.numTimes = :numTimes"),
	@NoSqlQuery(name="findByName", query="select * FROM TABLE e WHERE e.name=:name"),
	@NoSqlQuery(name="findByNumTimes", query="select * FROM TABLE e WHERE e.numTimes=:numTimes"),
	@NoSqlQuery(name="findByFloat", query="select * FROM TABLE e WHERE e.myFloat=:myFloat"),
	@NoSqlQuery(name="findByCool", query="select * FROM TABLE e WHERE e.isCool=:cool")
	
})
public class Activity {

	@Id
	private String id;
	
	@ManyToOne
	private Account account;
	
	@NoSqlIndexed
	private String uniqueColumn;
	
	@NoSqlIndexed
	private String name;
	@NoSqlIndexed
	private long numTimes;

	@NoSqlIndexed
	private Boolean isCool;
	@NoSqlIndexed 
	private float myFloat;
	
	private String somethingElse;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Account getAccount() {
		return account;
	}

	public void setAccount(Account account) {
		this.account = account;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	
	public String getUniqueColumn() {
		return uniqueColumn;
	}

	public void setUniqueColumn(String uniqueColumn) {
		this.uniqueColumn = uniqueColumn;
	}

	public long getNumTimes() {
		return numTimes;
	}

	public void setNumTimes(long numTimes) {
		this.numTimes = numTimes;
	}

	public float getMyFloat() {
		return myFloat;
	}

	public void setMyFloat(float myFloat) {
		this.myFloat = myFloat;
	}

	public String getSomethingElse() {
		return somethingElse;
	}

	public void setSomethingElse(String somethingElse) {
		this.somethingElse = somethingElse;
	}

	
	public Boolean getIsCool() {
		return isCool;
	}

	public void setIsCool(Boolean isCool) {
		this.isCool = isCool;
	}

	public static List<Activity> findBetween(Index<Activity> index, long from, long to) {
		Query<Activity> query = index.getNamedQuery("findBetween");
		query.setParameter("begin", from);
		query.setParameter("to", to);
		return query.getResultList();
	}
	
	public static Activity findSingleResult(Index<Activity> index, String key) {
		Query<Activity> query = index.getNamedQuery("findUnique");
		query.setParameter("unique", key);
		return query.getSingleObject();
	}
	public static List<Activity> findByName(Index<Activity> index, String name) {
		Query<Activity> query = index.getNamedQuery("findByName");
		query.setParameter("name", name);
		return query.getResultList();
	}

	public static List<Activity> findByCool(Index<Activity> index, boolean isCool) {
		Query<Activity> query = index.getNamedQuery("findByCool");
		query.setParameter("cool", isCool);
		return query.getResultList();
	}

	public static List<Activity> findNumTimes(Index<Activity> index, long numTimes) {
		Query<Activity> query = index.getNamedQuery("findByNumTimes");
		query.setParameter("numTimes", numTimes);
		return query.getResultList();
	}
	
	public static List<Activity> findByFloat(Index<Activity> index, float myFloat) {
		Query<Activity> query = index.getNamedQuery("findByFloat");
		query.setParameter("myFloat", myFloat);
		return query.getResultList();
	}
	
	public static List<Activity> findWithAnd(Index<Activity> index, String name, long numTimes) {
		Query<Activity> query = index.getNamedQuery("findWithAnd");
		query.setParameter("name", name);
		query.setParameter("numTimes", numTimes);
		return query.getResultList();
	}

	public static List<Activity> findWithOr(Index<Activity> index, String name, long numTimes) {
		Query<Activity> query = index.getNamedQuery("findWithOr");
		query.setParameter("name", name);
		query.setParameter("numTimes", numTimes);
		return query.getResultList();
	}

	public static List<Activity> findWithoutParens(Index<Activity> index,
			String name, long numTimes, float myFloat) {
		Query<Activity> query = index.getNamedQuery("findWithoutParens");
		query.setParameter("name", name);
		query.setParameter("numTimes", numTimes);
		query.setParameter("myFloat", myFloat);
		return query.getResultList();
	}

	public static List<Activity> findWithParens(Index<Activity> index,
			String name, long numTimes, float myFloat) {
		Query<Activity> query = index.getNamedQuery("findWithParens");
		query.setParameter("name", name);
		query.setParameter("numTimes", numTimes);
		query.setParameter("myFloat", myFloat);
		return query.getResultList();
	}
}
