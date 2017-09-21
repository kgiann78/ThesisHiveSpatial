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
        try (Connection con = DriverManager.getConnection("jdbc:hive2://" + host + ":10000/default?&hive.execution.engine=tez", username, password)) {
            System.out.println("Executing query: " + sql);
            Statement stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("\n-- OK --\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void executeQuery(String sql) {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        try (Connection con = DriverManager.getConnection("jdbc:hive2://" + host + ":10000", username, password)) {
            System.out.println("Executing query: " + sql);
            Statement stmt = con.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);

            for (int j = 1; j <= resultSet.getMetaData().getColumnCount(); j++) {
                System.out.print("|\t" + resultSet.getMetaData().getColumnLabel(j) + "\t|");
            }
            System.out.println("");

            if (resultSet.getFetchSize() > 0) {
                while (resultSet.next()) {
                    int i = 1;
                    for (; i < resultSet.getMetaData().getColumnCount(); i++) {
                        System.out.print("|\t" + resultSet.getObject(i) + "\t|");
                    }

                    System.out.println("|\t" + resultSet.getObject(i) + "\t|");
                }
            }
            System.out.println("\n-- OK --\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
