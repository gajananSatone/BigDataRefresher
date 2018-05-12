package gajanans.SparkJavaAnalytics;

import java.io.Serializable;

public class HappyPerson implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String name;
	private String favouriteBeverage;

	public HappyPerson() {}

	public HappyPerson(String n, String b) {
		name = n; favouriteBeverage = b;
	}

	public String getName() { return name; }

	public void setName(String n) { name = n; }
	public String getFavouriteBeverage() { return favouriteBeverage; }
	public void setFavouriteBeverage(String b) { favouriteBeverage = b; }
};
