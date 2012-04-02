package com.alvazan.test.db;

import com.alvazan.orm.api.anno.Id;
import com.alvazan.orm.api.anno.NoSqlEntity;

@NoSqlEntity
public class Account extends AccountSuper{

	@Id
	private String id;
	
	private String name;
	
	private long users;

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

	public long getUsers() {
		return users;
	}

	public void setUsers(long users) {
		this.users = users;
	}
}
