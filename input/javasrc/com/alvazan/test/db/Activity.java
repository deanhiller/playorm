package com.alvazan.test.db;

import java.util.List;

import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.Indexed;
import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;

@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findBetween", query="select *  FROM TABLE e WHERE e.numTimes >= :begin and e.numTimes < :to"),
	@NoSqlQuery(name="findUnique", query="select *  FROM TABLE e WHERE e.uniqueColumn = :unique")
	
})
public class Activity {

	@Id
	private String id;
	
	@ManyToOne
	private Account account;
	
	@Indexed
	private String uniqueColumn;
	
	@Indexed
	private String name;
	@Indexed
	private long numTimes;

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

	public String getSomethingElse() {
		return somethingElse;
	}

	public void setSomethingElse(String somethingElse) {
		this.somethingElse = somethingElse;
	}

	public static List<Activity> findBetween(Index<Activity> index, int from, int to) {
		Query<Activity> query = index.getNamedQuery("findBetween");
		query.setParameter("from", from);
		query.setParameter("to", to);
		return query.getResultList();
	}
	
	public static Activity findSingleResult(Index<Activity> index, String key) {
		Query<Activity> query = index.getNamedQuery("findUnique");
		query.setParameter("val", key);
		return query.getSingleObject();
	}
}
