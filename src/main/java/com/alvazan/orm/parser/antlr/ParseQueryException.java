package com.alvazan.orm.parser.antlr;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseQueryException extends RuntimeException {

	private static final Logger log = LoggerFactory.getLogger(ParseQueryException.class);
	private static final long serialVersionUID = 1L;

	private final String message;
	private final String tokenText;
	private final int characterPosition;
	
	public ParseQueryException() {
		this((String)null);
	}

	public ParseQueryException(String message) {
		super(message);
		this.message = null;
		tokenText = null;
		characterPosition = -1;
	}

	public ParseQueryException(RecognitionException cause) {
		this(null, cause);
	}

	public ParseQueryException(String message2, RecognitionException cause) {
		super(message2, cause);
		Tuple tuple = init(message2, cause);
		message = tuple.getMessage();
		tokenText = tuple.getTokenText();
		characterPosition = tuple.getCharacterPosition();
	}

	private Tuple init(String message2, RecognitionException cause) {
		int characterPosition = -1;
		String tokenText = null;
		String message = "";
		try {
			characterPosition = cause.charPositionInLine;
			message = "Unexpected token at character position="+characterPosition;
			Token token = cause.token;
			if(token != null) {
				tokenText = token.getText();
				message += ".  Unexpected token text="+token.getText();
			} else
				tokenText = null;
			Object node = cause.node;
			if(node != null)
				message += ". node="+node;
			if(message2 != null)
				message += " "+message2;
		} catch(Exception e) {
			if(log.isTraceEnabled())
				log.trace("Exception forming exc message", e);
			//could not form message
			message = "Bug, Could not form error correctly for this one(turn trace log on to see why)="+cause;
			if(message2 != null)
				message += " "+message2;
		}
		Tuple tuple = new Tuple(message, tokenText, characterPosition);
		return tuple;
	}

	@Override
	public String getMessage() {
		return message;
	}

	public String getTokenText() {
		return tokenText;
	}

	public int getCharacterPosition() {
		return characterPosition;
	}

	private static class Tuple {
		private String message;
		private String tokenText;
		private int characterPosition;
		public Tuple(String message2, String tokenText2, int characterPosition2) {
			this.message = message2;
			this.tokenText = tokenText2;
			this.characterPosition = characterPosition2;
		}
		public String getMessage() {
			return message;
		}
		public String getTokenText() {
			return tokenText;
		}
		public int getCharacterPosition() {
			return characterPosition;
		}
	}
}
