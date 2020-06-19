import org.apache.hive.jdbc.HiveStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

public class JDBCToHiveUtils {
    public static void main(String[] args) throws Exception {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        Class.forName(driverName);

        String url = "jdbc:hive2://localhost:10000/test";

        Properties info = new Properties();
//        info.setProperty("user", "bigdata");
//        info.setProperty("password", "******");

        Connection conn = DriverManager.getConnection(url, info);
        HiveStatement stmt = (HiveStatement) conn.createStatement();
        String table = "u_user";
        ResultSet result = stmt.executeQuery("SELECT COUNT(*) FROM " + table);
        String yarn_app_id = "";
        for (String log : stmt.getQueryLog(true, 10)) {
            System.out.println(log);
            if (log.contains("App id")) {
                yarn_app_id = log.substring(log.indexOf("App id") + 7, log.length() - 1);
            }
        }
        System.out.println("yarnAppId: " + yarn_app_id);
        Long count = result.getLong(1);
        System.out.println(count);
    }
}
