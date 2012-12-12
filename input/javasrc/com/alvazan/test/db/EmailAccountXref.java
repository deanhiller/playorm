package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;

@NoSqlEntity
public class EmailAccountXref {

	@NoSqlId
	private String id;
	
	@NoSqlManyToOne
	private Account account;
	
	@NoSqlManyToOne
	private User user;
	
	public EmailAccountXref() {}
	
	public EmailAccountXref(User user, Account acc) {
		this.user = user;
		this.account = acc;
		this.user.addAccount(this);
		this.account.addEmail(this);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Account getAccount() {
		return account;
	}

	public User getEmail() {
		return user;
	}
	
}
