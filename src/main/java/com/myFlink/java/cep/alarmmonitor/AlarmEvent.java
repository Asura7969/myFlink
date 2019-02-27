package com.myFlink.java.cep.alarmmonitor;

/**
 * Typical event from Network Element 
 * 
 */
public class AlarmEvent {
	int eventNumber;
	String resourceId;
	Severity severity;
	int probableCause;
	String specificProblem;
	long eventTime;

	public AlarmEvent(int eventNumber, String resourceId, Severity severity) {
		this.eventNumber = eventNumber;
		this.resourceId = resourceId;
		this.severity = severity;
		eventTime = System.currentTimeMillis();
	}

	public int getEventNumber() {
		return eventNumber;
	}

	public void setEventNumber(int eventNumber) {
		this.eventNumber = eventNumber;
	}

	public String getResourceId() {
		return resourceId;
	}

	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
	}

	public Severity getSeverity() {
		return severity;
	}

	public void setSeverity(Severity severity) {
		this.severity = severity;
	}

	public int getProbableCause() {
		return probableCause;
	}

	public void setProbableCause(int probableCause) {
		this.probableCause = probableCause;
	}

	public String getSpecificProblem() {
		return specificProblem;
	}

	public void setSpecificProblem(String specificProblem) {
		this.specificProblem = specificProblem;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	@Override
	public String toString() {
		return "AlarmEvent("+getResourceId()+"," +getEventNumber()+","+getSeverity().getName()+","+ getEventTime()+")";
	}

}