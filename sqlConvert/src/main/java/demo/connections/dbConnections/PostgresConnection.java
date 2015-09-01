package demo.connections.dbConnections;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import com.google.common.base.Stopwatch;

import demo.connections.IdbConnections.IConnection;
import demo.util.Logger;

public class PostgresConnection implements IConnection {

	private Connection conn = null;

	private String server = null;
	private String port = null;
	private String dbName = null;
	
	private String username  = null;
	private String password =null ;


	private Properties props;

	
	public static final String PROPERTIES_FILE_NAME = "database.properties";

	private final String  driver = "org.postgresql.Driver";
	
	public PostgresConnection() {
		super();
	}

	public PostgresConnection(String server, String port,String dbName , String username , String password) {
		super();
		this.server =server ;
		this.port = port ;
		this.dbName = dbName;
		this.username = username;
		this.password = password;
	}

	@Override
	public boolean initailize() {
		
		//Stopwatch timer = Stopwatch.createStarted();
		try {
			/*
			props = new Properties();

			InputStream in = PostgresConnection.class
					.getResourceAsStream(PROPERTIES_FILE_NAME);
			if (in == null) {
				Logger.errorLogger.error("Error: Failed to find the \"database.properties\" file. Note that it must be ");
				Logger.errorLogger.error("in the same directory as Main.class and that the name is case sensitive");
				return false;
			}
			props.load(in);

			// load the JDBC driver
			String drivers = props.getProperty("jdbc.drivers");
			if (drivers == null) {
				Logger.errorLogger.error("Error: No JDBC driver specified in the"
						+ "\"jdbc.driver\" attribute of the property file");
				return false;
			}
			*/
			Class.forName(driver);
		}

		
		 catch (Exception e) {
				Logger.errorLogger.catching(e);
				return false;
			} finally {
			//	Logger.infoLogger.info("Total time for initialazing connection : "
				//		+ timer.stop());
			}

		return true;
	}

	@Override
	public Connection connect() {

		Stopwatch timer = Stopwatch.createStarted();
		try {

			/*
			String url = props.getProperty("database.url");
			
			String database = (dbName.isEmpty() ? props
					.getProperty("database.name") : dbName.toLowerCase().trim());
			if (database == null) {
				Logger.errorLogger.debug("Error: No database name specified in the"
						+ "\"database.name\" attribute of the property file");
				return null;
			}
			
			String username = props.getProperty("database.user");
			if (username == null) {
				Logger.errorLogger.error("Error: No user name specified in the"
						+ "\"database.user\" attribute of the property file");
				return null;
			}
			*/
			// get the username and password via a Swing JDialog box
			// String[] userNamePassword = getUsernamePassword(username);
			//String password = props.getProperty("database.password");
			String url = "jdbc:postgresql://"+ server+":"+port+"/"+dbName.toLowerCase();
			
			Properties dbProb = new Properties();
			dbProb.setProperty("allowEncodingChanges", "true");
			dbProb.setProperty("user", username);
			dbProb.setProperty("password", password);
			
			//String[] userNamePassword = new String[] { username, password };
			// Get a database connection
			try {
				
					//conn = DriverManager.getConnection(database,
						//	userNamePassword[0], userNamePassword[1]);
				
				conn = DriverManager.getConnection(url,
						dbProb);
				
			} catch (Exception e) {
				Logger.errorLogger.catching( new Exception("Error: Attempt to connect to database \""
						+ dbName
						+ ((username == null) ? ""
								: (" with username \"" + username))
						+ "\" failed: " + e.getMessage()));
			}
		}  catch (Exception e) {
			Logger.errorLogger.catching(e);
			e.printStackTrace();
			System.exit(1);
		} finally {
			Logger.infoLogger.info("Total time for connecting to db : "
					+ timer.stop());
		}
		return conn;
	}

	@Override
	public boolean close() {
		Logger.debugLogger.debug("Closing connection");
		try {
			if (!conn.isClosed())
				conn.close();
			return true;

		} catch (Exception ex) {
			Logger.errorLogger.catching(ex);
			ex.printStackTrace();
			return false;
		}
	}

}
