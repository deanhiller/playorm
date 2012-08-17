package com.alvazan.test.db;

import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;


@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findBetween", query="select *  FROM TABLE e WHERE e.numTimes >= :begin and e.numTimes < :to"),
	@NoSqlQuery(name="findBetween2", query="select * FROM TABLE e WHERE e.numTimes > :begin and e.numTimes <= :to"),
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
	@NoSqlQuery(name="findByCool", query="select * FROM TABLE e WHERE e.isCool=:cool"),
	@NoSqlQuery(name="findAll", query="select * FROM TABLE e")
	
})
public class Activity {

	@NoSqlId
	private String id;
	
	@NoSqlManyToOne
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

	public static List<Activity> findBetween(NoSqlEntityManager mgr, long from, long to) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findBetween");
		query.setParameter("begin", from);
		query.setParameter("to", to);
		return query.getResultList();
	}
	
	public static List<Activity> findBetween2(NoSqlEntityManager mgr, long from, long to) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findBetween2");
		query.setParameter("begin", from);
		query.setParameter("to", to);
		return query.getResultList();
	}
	
	public static Activity findSingleResult(NoSqlEntityManager mgr, String key) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findUnique");
		query.setParameter("unique", key);
		return query.getSingleObject();
	}
	public static List<Activity> findByName(NoSqlEntityManager mgr, String name) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findByName");
		query.setParameter("name", name);
		return query.getResultList();
	}

	public static List<Activity> findByCool(NoSqlEntityManager mgr, boolean isCool) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findByCool");
		query.setParameter("cool", isCool);
		return query.getResultList();
	}

	public static List<Activity> findNumTimes(NoSqlEntityManager mgr, long numTimes) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findByNumTimes");
		query.setParameter("numTimes", numTimes);
		return query.getResultList();
	}
	
	public static List<Activity> findByFloat(NoSqlEntityManager mgr, float myFloat) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findByFloat");
		query.setParameter("myFloat", myFloat);
		return query.getResultList();
	}
	
	public static List<Activity> findWithAnd(NoSqlEntityManager mgr, String name, long numTimes) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findWithAnd");
		query.setParameter("name", name);
		query.setParameter("numTimes", numTimes);
		return query.getResultList();
	}

	public static List<Activity> findWithOr(NoSqlEntityManager mgr, String name, long numTimes) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findWithOr");
		query.setParameter("name", name);
		query.setParameter("numTimes", numTimes);
		return query.getResultList();
	}

	public static List<Activity> findWithoutParens(NoSqlEntityManager mgr,
			String name, long numTimes, float myFloat) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findWithoutParens");
		query.setParameter("name", name);
		query.setParameter("numTimes", numTimes);
		query.setParameter("myFloat", myFloat);
		return query.getResultList();
	}

	public static List<Activity> findWithParens(NoSqlEntityManager mgr,
			String name, long numTimes, float myFloat) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findWithParens");
		query.setParameter("name", name);
		query.setParameter("numTimes", numTimes);
		query.setParameter("myFloat", myFloat);
		return query.getResultList();
	}

	public static List<Activity> findAll(NoSqlEntityManager mgr) {
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findAll");
		return query.getResultList();		
	}
}
