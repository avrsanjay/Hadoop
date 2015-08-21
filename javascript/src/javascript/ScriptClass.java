package javascript;

public class ScriptClass {

	public static void main(String[] args) {
		for(int i=0;i<100;i++)
			System.out.println("insert into test_table(id,data) values ("+i+",\"hello this is test "+Math.random()+"\");");
	}

}
