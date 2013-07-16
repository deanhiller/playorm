package com.alvazan.test.db;

import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlQuery;

@NoSqlEntity
@NoSqlQuery(name="findAll", query="select e from TABLE as e")
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

	public static List<EmailAccountXref> findAll(NoSqlEntityManager mgr) {
		Query<EmailAccountXref> query = mgr.createNamedQuery(EmailAccountXref.class, "findAll");
		return query.getResultList(0, null);
	}
	
}
