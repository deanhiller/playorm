package com.alvazan.orm.api;

import java.util.List;
import java.util.Map;

public class IndexAddFailedException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	private final List<IndexErrorInfo> errors;

	public IndexAddFailedException() {
		super();
		errors = null;
	}

	public IndexAddFailedException(String message, Throwable cause) {
		super(message, cause);
		errors = null;
	}

	public IndexAddFailedException(String message) {
		super(message);
		errors = null;
	}

	public IndexAddFailedException(Throwable cause) {
		super(cause);
		errors = null;
	}

	public IndexAddFailedException(List<IndexErrorInfo> exceptions) {
		this.errors = exceptions;
	}

	@Override
	public String getMessage() {
		String msg = "Some writes to the index failed.  Items are persisted but NOT indexed now(rebuild your index after figuring out the error/problem).\n" +
				"  Call getExceptions to get the item that failed AND the Exception that caused the failure\n" +
				"In the meantime, here is a list of the items that failed and the message of failure...\n\n";
		
		for(IndexErrorInfo info : errors) {
			msg += "items list(total="+info.getItemsThatFailed().size()+")=\n";
			for(Map<String, String> item : info.getItemsThatFailed()) {
				msg += item+"\n";
			}
			msg+="failure="+info.getCause().getMessage()+"\n\n\n";
		}
		return msg;
	}

	public List<IndexErrorInfo> getExceptions() {
		return errors;
	}
	
}
