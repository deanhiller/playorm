package com.alvazan.orm.api.base.exc;

public class ChildWithNoPkException extends NoSqlOrmException {

	private static final long serialVersionUID = 1L;

	public ChildWithNoPkException() {
		super();
	}

	public ChildWithNoPkException(String arg0, Throwable arg1, boolean arg2,
			boolean arg3) {
		super(arg0, arg1, arg2, arg3);
	}

	public ChildWithNoPkException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public ChildWithNoPkException(String arg0) {
		super(arg0);
	}

	public ChildWithNoPkException(Throwable arg0) {
		super(arg0);
	}

	
}
