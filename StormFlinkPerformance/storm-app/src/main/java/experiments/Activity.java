package experiments;

import java.util.Date;

public class Activity {
	private int course_id;
	private String enroll_date;
	private String verb;
	private double result_score;
	
	public Activity(){
		
	}
	public  Activity ( int c, String e, String v, double r) {
		course_id = c;
		enroll_date = e;
		verb = v;
		result_score = r;
	}
	
	
	
	public int getCourse_id() {
		return course_id;
	}
	public void setCourse_id(int course_id) {
		this.course_id = course_id;
	}
	public String getEnroll_date() {
		return enroll_date;
	}
	public void setEnroll_date(String enroll_date) {
		this.enroll_date = enroll_date;
	}
	public String getVerb() {
		return verb;
	}
	public void setVerb(String verb) {
		this.verb = verb;
	}
	public double getResult_score() {
		return result_score;
	}
	public void setResult_score(double result_score) {
		this.result_score = result_score;
	}
	

}
