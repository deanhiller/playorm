package com.alvazan.play.logging;

public class Answer {

	private Integer answer;
	private int row;

	public Answer(Integer answer, int row) {
		this.answer = answer;
		this.row = row;
	}

	public Integer getAnswer() {
		return answer;
	}

	public void setAnswer(Integer answer) {
		this.answer = answer;
	}

	public int getRow() {
		return row;
	}

	public void setRow(int row) {
		this.row = row;
	}
	
}
