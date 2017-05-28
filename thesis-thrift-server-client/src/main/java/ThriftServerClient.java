import java.sql.*;

public class ThriftServerClient {
    private final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    private String host;
    private String username;
    private String password;

    public ThriftServerClient(String host, String username, String password) {
        this.host = host;
        this.username = username;
        this.password = password;
    }

    public void execute(String sql) {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        try (Connection con = DriverManager.getConnection("jdbc:hive2://" + host + ":10000", username, password)) {
            Statement stmt = con.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);

            if (resultSet.getFetchSize() > 0) {
                while (resultSet.next()) {
                    int i = 1;
                    for (; i < resultSet.getMetaData().getColumnCount(); i++) {
                        System.out.print(resultSet.getMetaData().getColumnLabel(i) + ": " + resultSet.getObject(i) + ", ");
                    }
                    System.out.println(resultSet.getMetaData().getColumnLabel(i) + ": " + resultSet.getObject(i));
                }
            }
            System.out.println("OK");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
