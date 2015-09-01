package demo.sqlConvert;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.jar.Attributes.Name;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Set;

import com.google.common.base.Stopwatch;

import demo.metamodel.IModel.ITable;
import demo.util.Helper;
import demo.util.Logger;

public class testData {

	// final String FILE =
	// "C:/Users/User/Documents/MscACS/SummerProject/SPWS/sqlConvert/data/";
	static final String FILE = "../../data/";
	static final int FETCH_SIZE = 1000;

	public static void processData(String dbName, Connection msConn,
			Connection postConn, ITable table) throws SQLException {
		Stopwatch timmer = Stopwatch.createStarted();
		// System.out.println("Start processing");
		// get column names list
		// System.out.println ("columns size " + table.getColumns().size());
		Set<String> columnList = table.getColumns().stream()
				.map(col -> col.toString()).collect(Collectors.toSet());
		// System.out.println ("columns  set size " +
		// table.getColumns().size());

		// join the columns names into one string
		String columnStr = columnList.stream().collect(Collectors.joining(","));

		System.out.println(columnStr);
		Logger.debugLogger.debug(columnStr);
		// create quetion marks list and fill it with ?
		List<String> questionMarkList = new ArrayList<String>();
		columnList.stream().forEach((name) -> {
			questionMarkList.add("?");
		});
		int marksNum = questionMarkList.size();
		String modifiedQuesMarkStr = "("
				+ questionMarkList.stream().collect(Collectors.joining(","))
				+ ")";

		// clear it to use it in another way :D :D
		questionMarkList.clear();

		// select data from table
		// System.out.println("Lists prepared");
		// System.out.println(table.getSchema() + "." + table.toString());

		String sql = "select " + columnStr + " from " + dbName + "."
				+ table.getSchema().toString() + "." + table.toString();
		System.out.println(sql);
		Logger.debugLogger.debug(sql);
		try (PreparedStatement st = msConn.prepareStatement(sql,
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

			// used for optimization to prevent fill the whole rest set into
			// database
			st.setFetchSize(1000);
			int tempsize = 0;
			try (ResultSet rs = st.executeQuery()) {
				List<List<Object>> fullList = new ArrayList<List<Object>>();

				while (rs.next()) {
					tempsize++;
					List<Object> objects = new ArrayList<Object>();
					questionMarkList.add(modifiedQuesMarkStr);
					columnList.stream().forEach(name -> {
						try {
							// System.out.print(rs.getObject(name)!= null ?
							// rs.getObject(name) + "," : "null ," );
							objects.add(rs.getObject(name.replace("\"", "")));
						} catch (Exception e) {
							e.printStackTrace();
							Logger.errorLogger.catching(e);
							for (StackTraceElement object : e.getStackTrace()) {
								Logger.errorLogger.error(e.getCause());
								Logger.errorLogger.error(e.getMessage());
								Logger.errorLogger.error(object.toString());
							}
						}// catch

					}// lamda
					); // foreach
					fullList.add(objects);

					// System.out.println("");

				}// while

				// create (? , ? ,?, ? ) , (? , ? , ? , ? ) ....
				String questionMarkStr = questionMarkList.stream().collect(
						Collectors.joining(","));

				// System.out.println("temp size : " + tempsize);
				// System.out.println("my question marks list size : "
				// + questionMarkList.size());
				// System.out.println("fill list size : "
				// + fullList.size());
				sql = "INSERT INTO " + table.getSchema().toString() + "."
						+ table.toString() + "(" + columnStr + ") VALUES "
						+ questionMarkStr + ";";
				// System.out.println("sql " + sql);
				Logger.debugLogger.debug(sql);
				// prepare to insert the values

				try (PreparedStatement pst = postConn.prepareStatement(sql)) {

					for (List<Object> myObjects : fullList) {
						int i = 1;
						for (Object object : myObjects) {
							pst.setObject(
									i
											+ (marksNum * fullList
													.indexOf(myObjects)),
									object, Types.OTHER);
							i++;
						}
					}

					pst.executeUpdate();

				} // try

			}// try result set

		} catch (Exception e) {
			Logger.errorLogger.catching(e);
			e.printStackTrace();
		}

		System.out.print("total inserting time :" + timmer.stop());
	}

	public static void processData2(String dbName, Connection msConn,
			Connection postConn, ITable table) throws SQLException {
		Stopwatch timmer = Stopwatch.createStarted();

		Set<String> columnList = table.getColumns().stream()
				.map(col -> col.toString()).collect(Collectors.toSet());

		// join the columns names into one string
		String columnStr = columnList.stream().collect(Collectors.joining(","));

		System.out.println(columnStr);
		Logger.debugLogger.debug(columnStr);

		// create quetion marks list and fill it with ?
		List<String> questionMarkList = new ArrayList<String>();
		columnList.stream().forEach((name) -> {
			questionMarkList.add("?");
		});

		int marksNum = questionMarkList.size();
		String modifiedQuesMarkStr = "("
				+ questionMarkList.stream().collect(Collectors.joining(","))
				+ ")";

		// clear it to use it in another way :D :D

		// select data from table
		// System.out.println("Lists prepared");
		// System.out.println(table.getSchema() + "." + table.toString());

		String sql = "select " + columnStr + " from " + dbName + "."
				+ table.getSchema().toString() + "." + table.toString();
		System.out.println(sql);
		Logger.debugLogger.debug(sql);
		try (PreparedStatement st = msConn.prepareStatement(sql,
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

			// used for optimization to prevent fill the whole rest set into
			// database
			st.setFetchSize(1000);
			int tempsize = 0;
			try (ResultSet rs = st.executeQuery()) {

				while (rs.next()) {
					tempsize++;
					List<Object> cells = new ArrayList<Object>();
					columnList.stream().forEach(name -> {
						try {
							// System.out.print(rs.getObject(name)!= null ?
							// rs.getObject(name) + "," : "null ," );
							cells.add(rs.getObject(name.replace("\"", "")));
						} catch (Exception e) {
							e.printStackTrace();
							Logger.errorLogger.catching(e);
							for (StackTraceElement object : e.getStackTrace()) {
								Logger.errorLogger.error(e.getCause());
								Logger.errorLogger.error(e.getMessage());
								Logger.errorLogger.error(object.toString());
							}
						}// catch

					}// lamda
					); // foreach

					// System.out.println("");

					sql = "INSERT INTO " + table.getSchema().toString() + "."
							+ table.toString() + "(" + columnStr + ") VALUES "
							+ modifiedQuesMarkStr + ";";
					// System.out.println("sql " + sql);
					Logger.debugLogger.debug(sql);
					// prepare to insert the values

					try (PreparedStatement pst = postConn.prepareStatement(sql)) {

						int i = 1;
						for (Object object : cells) {
							pst.setObject(i, object, Types.OTHER);
							i++;
						}

						pst.executeUpdate();

					} // try

				}// while
			}// try result set

		} catch (Exception e) {
			Logger.errorLogger.catching(e);
			e.printStackTrace();
		}

		System.out.print("total inserting time :" + timmer.stop());
	}

	public static void processData3(String dbName, Connection msConn,
			Connection postConn, ITable table) throws SQLException {
		 Stopwatch timmer = Stopwatch.createStarted();

		// get column names list
		Set<String> columnList = table.getColumns().stream()
				.map(col -> col.toString()).collect(Collectors.toSet());

		// ms sql format columns
		String columnStrMs = columnList.stream()
				.map(col -> "[".concat(col.replace("\"", "")).concat("]"))
				.collect(Collectors.joining(","));

		// postgres columns format
		String columnStr = columnList.stream().collect(Collectors.joining(","));

		// Logger.debugLogger.debug(columnStr);

		// create quetion marks list and fill it with ?
		List<String> questionMarkList = new ArrayList<String>();
		columnList.stream().forEach((name) -> {
			questionMarkList.add("?");
		});

		int marksNum = questionMarkList.size();
		String modifiedQuesMarkStr = "("
				+ questionMarkList.stream().collect(Collectors.joining(","))
				+ ")";

		// clear it to use it in another way :D :D
		questionMarkList.clear();

		// select data from table
		String sql = "select " + columnStrMs + " from " + dbName + "."
				+ table.getSchema() + "." + table;
		// System.out.println(sql);
		Logger.debugLogger.debug(sql);
		try (PreparedStatement st = msConn.prepareStatement(sql,
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

			// used for optimization to prevent fill the whole rest set into
			// database
			st.setFetchSize(FETCH_SIZE);
			int count = 0;
			try (ResultSet rs = st.executeQuery()) {
				List<List<Object>> fullList = new ArrayList<List<Object>>();

				while (rs.next()) {
					count++;
					List<Object> objects = new ArrayList<Object>();
					questionMarkList.add(modifiedQuesMarkStr);

					/*
					 * columnList.stream().forEach(name -> { try { //
					 * System.out.print(rs.getObject(name)!= null ? //
					 * rs.getObject(name) + "," : "null ," );
					 * objects.add(rs.getObject(name.replace("\"", ""))); }
					 * catch (Exception e) { e.printStackTrace();
					 * Logger.errorLogger.catching(e); for (StackTraceElement
					 * object : e.getStackTrace()) {
					 * Logger.errorLogger.error(e.getCause());
					 * Logger.errorLogger.error(e.getMessage());
					 * Logger.errorLogger.error(object.toString()); } }// catch
					 * 
					 * }// lamda ); // foreach
					 */

					Iterator<String> colItretor = columnList.iterator();
					while (colItretor.hasNext()) {
						objects.add(rs.getObject(colItretor.next().replace(
								"\"", "")));
					}

					fullList.add(objects);

					if (count == FETCH_SIZE) {
						insertToPostgres(table, columnStr, fullList, marksNum,
								questionMarkList, postConn);
						count = 0;
						fullList.clear();
						questionMarkList.clear();
					}

				}// while

				if (count != 0)
					insertToPostgres(table, columnStr, fullList, marksNum,
							questionMarkList, postConn);

			}// try result set

			 Logger.infoLogger.info("total inserting time : " + table.getSchema()
			 + "." + table + " "+timmer.stop());

		} catch (Exception e) {
			Logger.errorLogger.error("error at table :" + table);
			Logger.errorLogger.catching(e);
			e.printStackTrace();
		}
	}

	private static void insertToPostgres(ITable table, String columnStr,
			List<List<Object>> fullList, int marksNum,
			List<String> questionMarkList, Connection postConn)
			throws Exception {
		// create (? , ? ,?, ? ) , (? , ? , ? , ? ) ....
		String questionMarkStr = questionMarkList.stream().collect(
				Collectors.joining(","));

		String sql = "INSERT INTO " + table.getSchema() + "." + table + "("
				+ columnStr + ") VALUES ";
		// System.out.println("sql " + sql);
		Logger.debugLogger.debug(sql);
		// prepare to insert the values
		sql += questionMarkStr + ";";
		try (PreparedStatement pst = postConn.prepareStatement(sql)) {
			/*
			 * for (List<Object> myObjects : fullList) { int i = 1;
			 * 
			 * for (Object object : myObjects) { int index = i + (marksNum *
			 * fullList.indexOf(myObjects)); if (table.toString()
			 * .equalsIgnoreCase("NewFactCurrencyRate"))
			 * Logger.debugLogger.debug("i : "+i+",index : " + index +
			 * ", value : " + object); pst.setObject(index, object,
			 * Types.OTHER); i++; } }
			 */
			Iterator<List<Object>> myObjects = fullList.iterator();
			int listIndex = 0;
			while (myObjects.hasNext()) {
				int objectIndex = 1;
				Iterator<Object> objects = myObjects.next().iterator();
				while (objects.hasNext()) {
					int index = objectIndex + (marksNum * listIndex);

					pst.setObject(index, objects.next(), Types.OTHER);
					objectIndex++;
				}
				listIndex++;
			}

			pst.executeUpdate();

		} // try
	}

	// this method has limitations , no field should contain a null value ,
	// postgres server do not understand 0x00 or 0xff as null values or field
	// return
	// using option -c gives almost the same result as using jdbc to insert data
	// using option -N or -w is much faster but we might have some problems with
	// encoding
	public static void bulkprocess(String dbName, String serverAndInstance,
			Connection msConn, Connection postConn, ITable table) {

		Set<String> columnList = table.getColumns().stream()
				.map(col -> col.toString()).collect(Collectors.toSet());

		// join the columns names into one string and reformat them to match
		// mssql server syntax
		String columnStr = columnList.stream()
				.map(col -> "[".concat(col.replace("\"", "")).concat("]"))
				.collect(Collectors.joining(","));

		String filePath = testData.class.getClassLoader().getResource("")
				.getPath()
				+ FILE;
		filePath = filePath.substring(1);
		// String filePath = FILE ;

		String outFile = filePath + table.getSchema().toString() + "_"
				+ table.toString() + ".txt";
		String errorFile = filePath + "error.log";
		String cmd = "bcp \"SELECT " + columnStr + " FROM " + dbName + "."
				+ table.getSchema().toString() + "." + table.toString()
				+ "\" queryout " + outFile + " -c -S " + serverAndInstance
				+ " -T "; // -a 65535";// -e " + errorFile ;
		Logger.infoLogger.info(cmd);

		Runtime runTime = Runtime.getRuntime();
		Process p = null;
		try {
			p = runTime.exec(cmd);

			String line;

			InputStream inputStream = p.getInputStream();
			InputStreamReader inputStreamReader = new InputStreamReader(
					inputStream);
			try (BufferedReader bufferedReader = new BufferedReader(
					inputStreamReader)) {

				while ((line = bufferedReader.readLine()) != null) {
					System.out.println(line);
					Logger.infoLogger.info(table.getSchema().toString() + "."
							+ table.toString() + " : " + line);
				}

				/*
				 * try { Thread.sleep(30); } catch (InterruptedException e) { //
				 * TODO Auto-generated catch block e.printStackTrace(); }
				 */
				// processFile(outFile);

				copyToPostgres(table.getSchema().toSQL(), table.toString(),
						columnList, postConn, outFile);
			}
			inputStream = p.getErrorStream();
			inputStreamReader = new InputStreamReader(inputStream);
			try (BufferedReader bufferedReader2 = new BufferedReader(
					inputStreamReader)) {
				line = "";
				while ((line = bufferedReader2.readLine()) != null) {
					System.out.println(line);
					Logger.errorLogger.error(table.getSchema() + "."
							+ table.toString() + " : " + line);
				}
			}
			System.out.println("bcp executed succefully for file" + filePath);

		} catch (IOException e) {
			e.printStackTrace();
			Logger.debugLogger.catching(e);
		}
	}

	private static void processFile(String outFile) {

		System.out.println(Paths.get(outFile));
		CharsetDecoder dec = StandardCharsets.US_ASCII.newDecoder()
				.onMalformedInput(CodingErrorAction.IGNORE);
		try (Stream<String> lines = Files.lines(Paths.get(outFile))) {
			List<String> newLines = lines.parallel()
					.map(line -> line = line.replace("\0", "\\."))
					.collect(Collectors.toList());
			// Files.newBufferedWriter(Paths.get(outFile),
			// Charset.defaultCharset(),
			// StandardOpenOption.WRITE).write(newLines.collect(Collectors.joining("")));
			// System.out.println("number of lines " + lines.count());
			newLines.stream().forEach(line -> {
				try {
					// Logger.warnLogger.warn(line.toString());
					Files.write(Paths.get(outFile), line.getBytes(),
							StandardOpenOption.WRITE);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void copyToPostgres(String schemaName, String tableName,
			Set<String> ColNames, Connection postConn, String fileUrl) {

		String columnStr = ColNames.stream().collect(Collectors.joining(","));

		String sql = "COPY " + schemaName + "." + tableName + " (" + columnStr
				+ ")" + " FROM '" + fileUrl + "'" + " WITH"
				+ " ENCODING 'LATIN1'"; // 'SQL_ASCII'"; // CSV ESCAPE '\"' ";

		try {
			if (!postConn.isClosed()) {
				System.out.print(sql);
				try (PreparedStatement ps = postConn.prepareStatement(sql)) {
					if (ps.execute())
						System.out.println("Data transfered to " + schemaName
								+ "." + tableName);
					else
						System.out.println("Data transfered failed to "
								+ schemaName + "." + tableName);

				}

			} else
				System.out.println("postgres connection is closed");
		} catch (Exception e) {
			e.printStackTrace();
			Logger.errorLogger.catching(e);
		}
	}

}

// http://javabcp-ashish.blogspot.co.uk/2010/10/java-code-to-run-bcpin-for-multiple.html
