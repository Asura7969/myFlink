package com.myFlink.java.cep.alarmmonitor;

public enum Severity {
	CRITICAL(1, "Critical"), MAJOR(2, "Major"), MINOR(3, "Minor"), WARNING(4, "Warning"), CLEAR(5, "Clear");

	private int value;
	private String name;

	Severity(int value, String name) {
		this.setValue(value);
		this.setName(name);
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}