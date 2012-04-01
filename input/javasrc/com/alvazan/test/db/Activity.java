package com.alvazan.test.db;

import com.alvazan.orm.api.anno.Id;
import com.alvazan.orm.api.anno.ManyToOne;
import com.alvazan.orm.api.anno.NoSqlEntity;

@NoSqlEntity
public class Activity {

	@Id
	private String id;
	
	@ManyToOne
	private Account account;
	
	private String name;
	
	private long numTimes;

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

	public long getNumTimes() {
		return numTimes;
	}

	public void setNumTimes(long numTimes) {
		this.numTimes = numTimes;
	}
	
}
