package demo.connections.dbConnections;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import com.google.common.base.Stopwatch;

import demo.connections.IdbConnections.IConnection;
import demo.util.Logger;

public class MSSQLConnection implements IConnection {

	public static final String PROPERTIES_FILE_NAME = "MSSqlDatabase.properties";

	public static final String DB_NAME_PROP = "database.name";
	public static final String DB_DRIVER_PROP = "jdbc.drivers";
	public static final String DB_USER_NAME_PROP = "database.user";
	public static final String DB_PASSWORD_PROP = "database.password";
	public static final String DB_SCHEMA_PROP = "database.schema";
	public static final String DB_URL = "database.url";

	private Connection conn = null;

	private Properties props;

	private boolean integratedSecurity ;
	
	private String server = null;
	private String port = null;
	private String dbName = null;
	
	private String username  = null;
	private String password =null ;
	
	private final String  driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	public MSSQLConnection(String server , String port ,String dbName,boolean integratedSecurity, String ...param) {
			
		this.server = server;
		this.port = port;
		this.dbName = dbName;
		this.integratedSecurity = integratedSecurity;
		if (!this.integratedSecurity)
		{
			username = param[0];
			password = param[1];
		}
	}


	/*
	 * @return properties
	 */
	public Properties getProps() {
		return props;
	}
	@Override
	public boolean initailize() {
		/*
		Stopwatch timer = Stopwatch.createStarted();
		// Load in Database properties from file
		props = new Properties();

		// try to load the last saved properties
		try (InputStream in = MSSQLConnection.class
				.getResourceAsStream(PROPERTIES_FILE_NAME)) {
			// if no new properties exists load the default ones

			if (in == null) {
				Logger.errorLogger
						.error("Error: Failed to find the \"database.properties\" file. Note that it must be ");
				Logger.errorLogger
						.error("in the same directory as Main.class and that the name is case sensitive");
				System.exit(1);
				return false;
			}

			props.load(in);
			// close stream
			in.close();

			Logger.debugLogger.debug("Connection initialized");
			return true;
		} catch (Exception e) {
			Logger.errorLogger.catching(e);
			return false;
		} finally {
			Logger.infoLogger.info("Total time for initialazing connection : "
					+ timer.stop());
		}
		*/
		return true;

	}

	/*
	 * @return connection
	 */
	public Connection connect() {
		Stopwatch timer = Stopwatch.createStarted();
		try {

			// get the db info from properties
			/*
			String[] databaseInfo = getDBConnectionInfo(props);
			if (databaseInfo == null) {
				Logger.errorLogger
						.error("Database connecting infromation is null");
				System.exit(1);
			}
*/
			// load the jdbc driver
			//Class.forName(databaseInfo[0]);
			Class.forName(this.driver);
			// Get a database connection
		//	String fullDatabaseURL = null;
			try {
				/*
				if (!databaseInfo[1].endsWith("/"))
					databaseInfo[1] = databaseInfo[1].concat("/");

				fullDatabaseURL = databaseInfo[1] + databaseInfo[2];// +":"+databaseInfo[3];
				Logger.debugLogger
						.debug("fullDatabaseURL : " + fullDatabaseURL);

				String connectionUrl = "jdbc:sqlserver://localhost:49524;"
						+ "databaseName=AdventureWorks2014;integratedSecurity=true;";
*/
				String connectionUrl = "jdbc:sqlserver://"+this.server+":"+port+";"
						+ "databaseName="+this.dbName+";"; 
				
				if(this.integratedSecurity)
					connectionUrl +="integratedSecurity=true;";
				else 
					connectionUrl += "user="+this.username+";"+"password=" +this.password+";";
				conn = DriverManager.getConnection(connectionUrl);
				Logger.debugLogger.debug(connectionUrl);
				Logger.debugLogger.debug("Connected");

			} catch (Exception e) {
				e.printStackTrace();
				Logger.errorLogger.catching(e);
				Logger.errorLogger
						.catching(new Exception(
								"Error: Attempt to connect to database \""
										+ dbName
										+ ((this.username == null) ? ""
												: (" with username \"" + this.username))
										+ "\" failed: " + e.getMessage()));
			}

		} catch (Exception e) {
			Logger.errorLogger.catching(e);
			e.printStackTrace();
			System.exit(1);
		} finally {
			Logger.infoLogger.info("Total time for connecting to db : "
					+ timer.stop());
		}
		return conn;

	}

	/*
	 * close the connection with database
	 */
	public boolean close() {
		Logger.debugLogger.debug("Closing connection");
		try {
			if (conn != null)
				if (!conn.isClosed())
					conn.close();
			return true;
		} catch (Exception ex) {
			Logger.errorLogger.catching(ex);
			return false;
		}
	}

	/*
	 * build the db info UI
	 * 
	 * @param props, properties contain db info
	 * 
	 * @return String
	 */
	private static String[] getDBConnectionInfo(Properties props) {
		// get the JDBC driver
		String drivers = props.getProperty(DB_DRIVER_PROP);
		// get the db url
		String databaseUrl = props.getProperty(DB_URL);
		// get the db Name
		String database = props.getProperty(DB_NAME_PROP);
		// get the schema
		String schema = props.getProperty(DB_SCHEMA_PROP);
		// get the user name
		String username = props.getProperty(DB_USER_NAME_PROP);
		// get the password
		String pass = props.getProperty(DB_PASSWORD_PROP);

		return new String[] { drivers, databaseUrl, database, schema, username,
				pass };

	}

}
