package experiments;

public class StudentActivity {
	private String id;
	private int student_id;
	private int university_id;
	private Activity course_details;
	
	public StudentActivity(){
		
	}
	
	public StudentActivity(String i, int s, int u , Activity a){
		id = i;
		student_id = s;
		university_id = u;
		course_details = a;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getStudent_id() {
		return student_id;
	}

	public void setStudent_id(int student_id) {
		this.student_id = student_id;
	}

	public int getUniversity_id() {
		return university_id;
	}

	public void setUniversity_id(int university_id) {
		this.university_id = university_id;
	}

	public Activity getCourse_details() {
		return course_details;
	}

	public void setCourse_details(Activity course_details) {
		this.course_details = course_details;
	}
	
	

}
