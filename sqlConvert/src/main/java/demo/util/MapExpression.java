package demo.util;

import java.lang.invoke.SwitchPoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapExpression {

	private static List<String> sqlFunctions = null;

	private static void initializeFunctionsList() {
		sqlFunctions = new ArrayList<String>();
		sqlFunctions.add("dateAdd".toLowerCase());
		sqlFunctions.add("getdate".toLowerCase());
		sqlFunctions.add("like".toLowerCase());

	}

	private static List<String> getFunctionsList() {
		if (sqlFunctions == null)
			initializeFunctionsList();
		return sqlFunctions;
	}

	public static String getFormatedExpression(String str, String colType) {
		String formatedStr = getFormatedExpression(str);

		formatedStr = getTypeCasting(formatedStr, colType);
		Logger.debugLogger.debug("Formated expression:" +formatedStr);
		return formatedStr;
	}

	/*
	 * private static String getFormatedExpression(String str) {
	 * 
	 * System.out.print("String to format " + str); for (String item :
	 * getFunctionsList()) { if (str.toLowerCase().contains(item)) {
	 * 
	 * int firstIndex; int lastIndex;
	 * 
	 * firstIndex = str.indexOf(item); lastIndex = str.indexOf(')');
	 * 
	 * String temp = (lastIndex > 0 ? str.substring(firstIndex, lastIndex) :
	 * str.substring(firstIndex)); String remain = str
	 * .substring(str.indexOf(temp) + temp.length());
	 * 
	 * System.out.println("temp : " + temp); System.out.println("remain : " +
	 * remain);
	 * 
	 * long count1 = temp.chars().filter(num -> (char) num == '(') .count();
	 * 
	 * long count2 = temp.chars().filter(num -> (char) num == ')') .count();
	 * 
	 * System.out.println("count 1 , 2 : " + count1 + ", " + count2);
	 * 
	 * while (count1 != count2) {
	 * 
	 * temp = temp.concat(remain.substring(0, remain.indexOf(')') + 1)); remain
	 * = remain.substring(remain.indexOf(')') + 1);
	 * 
	 * System.out.println("temp2 : " + temp); System.out.println("remain 2 : " +
	 * remain);
	 * 
	 * count1 = temp.chars().filter(num -> (char) num == '(') .count();
	 * 
	 * count2 = temp.chars().filter(num -> (char) num == ')') .count();
	 * 
	 * System.out.println("count 1 , 2 : " + count1 + ", " + count2); }
	 * 
	 * // dateadd(year,(-18),getdate()) System.out.println("temp : " + temp);
	 * System.out.println("remain : " + remain); str = str.substring(0,
	 * str.indexOf(temp));
	 * 
	 * str += " " + format(temp, item) + " " + getFormatedExpression(remain);
	 * System.out.println(str); return str; } }
	 * 
	 * return str; }
	 */

	private static String getFormatedExpression(String str) {

		String formated = str;
		Pattern p = Pattern
				.compile("(dateAdd|dateadd|DateAdd)\\((\\w+),\\((-)*\\d+\\),.+\\)");
		Matcher m = p.matcher(str);
		for (int i = 0; i < m.groupCount(); i++) {
			while (m.find()) {
				formated = formated.replace(m.group(),
						format(m.group(), "dateadd"));

			}

		}

		formated = format2("(getdate\\(\\s?\\))", formated,
				"current_timestamp", true, null);
		formated = format2("(like '\\[)", formated, "~ '[", true, null);
		
		formated = format2("((STDEV|stdev)\\s*?\\(.+\\))", formated, "stddev",
				false, "STDEV");
		formated = format2("((stdevp|STDEVP)\\s*?\\(.+\\))", formated,
				"stddev_pop", false, "STDEVP");
		formated = format2("((var|VAR)\\s*?\\(.+\\))", formated, "variance",
				false, "VAR");
		formated = format2("((varp|VARP)\\s*?\\(.+\\))", formated,
				"var_pop", false, "VARP");

		return formated;
	}

	private static String format2(String pattern, String fullString,
			String replacingStr, boolean replacePattern, String strToReplace) {
		String formated = fullString;
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(fullString);
		for (int i = 0; i < m.groupCount(); i++) {
			while (m.find()) {

				formated = formated.replace(
						m.group(),
						replacePattern ? replacingStr : m.group().replace(
								strToReplace, replacingStr));

			}

		}
		return formated;
	}

	private static String format(String str, String key) {
		switch (key) {
		case "dateadd": {
			str = str.trim().substring(str.indexOf(key) + key.length());
			str = str.trim().substring(str.indexOf('(') + 1,
					str.lastIndexOf(')'));
			String[] temp = str.split(",");
			// do someting here

			String convertedFun = getFormatedExpression(temp[2]) + " + '"
					+ temp[1] + " " + temp[0] + "'";
			return convertedFun;

		}
		/*
		 * case "getdate": return "current_timestamp";
		 * 
		 * case "like": String formated = str; Pattern p =
		 * Pattern.compile("(like '\\[)"); Matcher m = p.matcher(str); for (int
		 * i = 0; i < m.groupCount(); i++) { while (m.find()) { formated =
		 * formated.replace(m.group(), "~ '[");
		 * 
		 * }
		 * 
		 * }
		 * 
		 * return formated;
		 */
		}
		return null;

	}

	private static String getTypeCasting(String str, String colType) {
		String temp = str;
		switch (colType.toLowerCase().trim()) {
		case "money":
			Pattern p = Pattern.compile("(\\d+\\.?\\d*)");
			Matcher m = p.matcher(str);

			for (int i = 0; i < m.groupCount(); i++) {
				while (m.find()) {
					temp = temp.replace(m.group(), "money (" + m.group() + ")");

				}

			}

			break;

		default:
			break;
		}
		return temp;
	}
}
