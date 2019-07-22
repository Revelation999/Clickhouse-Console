import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class ClickhouseTest 
{
	static Connection connection;
	static Statement statement;
	static ResultSet results;
	static String address;
	private static final String DEFAULTADDRESS = "jdbc:clickhouse://192.168.150.29:8123/default";
	public static void main(String[] args) throws SQLException
	{
		address = args.length > 0 ? args[0] : DEFAULTADDRESS;
		connection = DriverManager.getConnection(address);
		statement = connection.createStatement();
		
		Scanner userInput = new Scanner(System.in);
		String sql;
		while (!(sql = userInput.nextLine()).equals("exit"))
			exeSql(sql);
		
		connection.close();
		statement.close();
		results.close();
		userInput.close();
	}
	
	public static void exeSql(String command)
	{
		try {
			results = statement.executeQuery(command);
            ResultSetMetaData rsmd = results.getMetaData();
            List<Map <String, String>> list = new ArrayList <Map <String, String>>();
            while (results.next())
            {
            	Map<String, String> map = new HashMap <String, String>();
            	for (int i = 1; i <= rsmd.getColumnCount(); i++)
            		map.put(rsmd.getColumnName(i), results.getString(rsmd.getColumnName(i)));
            	list.add(map);
            }
            for (Map<String, String> map : list)
            	System.out.println(map);
		} catch (SQLException e) {
			System.err.println("Syntax error");
		}
	}
}
