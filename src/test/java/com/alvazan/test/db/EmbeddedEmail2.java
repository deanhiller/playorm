package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlEmbeddable;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;

@NoSqlEmbeddable
public class EmbeddedEmail2 extends EmbeddedEmail {
	private static final String MAIN = "main";

	@NoSqlId
	private int idkey;

	@NoSqlIndexed
	private String name;

	@NoSqlIndexed
	private String type;

	private boolean something = true;

	private int number = 36;

	public int getIdkey() {
		return idkey;
	}

	public void setIdkey(int idkey) {
		this.idkey = idkey;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public boolean isSomething() {
		return something;
	}

	public void setSomething(boolean something) {
		this.something = something;
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}
}
