package com.alvazan.test.db;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.base.anno.NoSqlEmbedded;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
@NoSqlEntity
public class Email {

	@NoSqlId
	private String id; 
	
/*	@NoSqlEmbedded
	private List<Email> emails = new ArrayList<Email>();*/
	
	@NoSqlEmbedded
	private List<String> ids = new ArrayList<String>();
	@NoSqlEmbedded
	private List<Integer> ints = new ArrayList<Integer>();

/*	@NoSqlEmbedded
	private Email email;*/

	@NoSqlIndexed
	private String name;
	
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

/*	public List<Email> getEmails() {
		return emails;
	}

	public void setEmails(List<Email> emails) {
		this.emails = emails;
	}

	public Email getEmail() {
		return email;
	}

	public void setEmail(Email email) {
		this.email = email;
	}*/

	public List<String> getIds() {
		return ids;
	}

	public void setIds(List<String> ids) {
		this.ids = ids;
	}

	public List<Integer> getInts() {
		return ints;
	}

	public void setInts(List<Integer> ints) {
		this.ints = ints;
	}
	
}
