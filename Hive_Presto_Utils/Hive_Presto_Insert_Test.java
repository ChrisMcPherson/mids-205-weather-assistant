import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

public class HiveInsert {
    
    public Connection getPrestoConnection() throws Exception {
        Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        Connection c = DriverManager.getConnection("jdbc:presto://ec2-54-152-39-8.compute-1.amazonaws.com:8889/hive", "hadoop", "");
        return c;
    }
    
    public Connection getHiveConnection() throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection c = DriverManager.getConnection("jdbc:hive2://ec2-54-152-39-8.compute-1.amazonaws.com:10000/w205_final", "hadoop", "");
        c.setAutoCommit(true);
        return c;
    }
    
    public String getHiveSQLQueryLocal() {
        StringBuffer sql = new StringBuffer();
        sql.append("INSERT INTO w205_final.current_weather_local ");
        sql.append("(id, city_id, city_name, dt, station_name,");
        sql.append("coord_lon, coord_lat,");
        sql.append("temp, temp_min, temp_max, humidity, pressure, wind_speed, wind_gust, wind_degree,");
        sql.append("rain1h, rain3h,pct_cloud, sunrise, sunset,");
        sql.append("weather_name, weather_description, weather_icon, weather_code,");
        sql.append("retrieval_date, hour, file_name) ");
        sql.append(", (");
        sql.append("1, ");
        sql.append("32, ");
        sql.append("'testCity'" + ", ");
        sql.append("timestamp '2016-06-27 07:21:27'" + ", ");
        sql.append("'cmc stations'" + ",");
        sql.append("124.7 " + ", ");
        sql.append("10.95" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("timestamp '2016-06-26 21:21:28'" + ", ");
        sql.append("timestamp '2016-06-26 21:21:28'" + ", ");
        sql.append("'rain'" + ", ");
        sql.append("'rain'" + ",");
        sql.append("'rain'" + ",");
        sql.append("'rain'" + ",");
        sql.append("'06-27-2016/0/1683852-currrent-weather-00-33-53.json'" + ",");
        sql.append("'06-27-2016/0/1683852-currrent-weather-00-33-53.json'" + ",");
        sql.append("'06-27-2016/0/1683852-currrent-weather-00-33-53.json'" + ")");
        return sql.toString();
    }
    
    public String getPrestoSQLQuery() {
        StringBuffer sql = new StringBuffer();
        sql.append("INSERT INTO w205_final.current_weather ");
        sql.append("VALUES (");
        sql.append("1, ");
        sql.append("32, ");
        sql.append("'testCity'" + ", ");
        sql.append("timestamp '2016-06-27 07:21:27'" + ", ");
        sql.append("'cmc stations'" + ",");
        sql.append("124.7 " + ", ");
        sql.append("10.95" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("301.17" + ", ");
        sql.append("timestamp '2016-06-26 21:21:28'" + ", ");
        sql.append("timestamp '2016-06-26 21:21:28'" + ", ");
        sql.append("'rain'" + ", ");
        sql.append("'rain'" + ",");
        sql.append("'rain'" + ",");
        sql.append("'rain'" + ",");
        sql.append("'06-27-2016/0/1683852-currrent-weather-00-33-53.json'" + ",");
        sql.append("'06-27-2016/0/1683852-currrent-weather-00-33-53.json'" + ",");
        sql.append("'06-27-2016/0/1683852-currrent-weather-00-33-53.json'" + ")");
        return sql.toString();
    }
    
    public void writeToDB(String sql) {
        Connection c = null;
        Statement st = null;
        ResultSet rs = null;
        // String sql = getSQlQuery();
        try {
            c = getPrestoConnection(); //currently configured to test Presto, the decided query engine over Hive
            
            System.out.println(sql);
            st = c.createStatement();
            st.executeUpdate(sql);
            
        } catch (Exception ex) {
            System.out.println("Sql " + sql);
            ex.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (c != null) {
                    c.close();
                }
                
            } catch (Exception ce) {
                // nothing
            }
        }
    }
    
    public static void main(String args[]) {
        HiveInsert testHive = new HiveInsert();
        String sql = testHive.getPrestoSQLQuery();
        Date date = new Date();
        
        Integer startMilli = (int) date.getTime();
        System.out.println("start presto insert: " + date.toString());
        testHive.writeToDB(sql);
        Date date2 = new Date();
        Integer endMilli = (int) date2.getTime();
        System.out.println(endMilli - startMilli + " milliseconds");
    }
}
