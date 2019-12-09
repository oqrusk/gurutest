package org.embulk.filter.reverse_geocoding;

import ch.hsr.geohash.GeoHash;
import com.google.common.base.Strings;
import org.h2.tools.Csv;

import java.net.URL;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;

public class GeoCodeMap
{

    static final String LEVEL5_CSV_PATH = "jpcities-master/geohash5.csv";
    static final String LEVEL4_CSV_PATH = "jpcities-master/geohash4.csv";
    static HashMap<String, String[]> GEOHASH_MAP;
    //    GeoHash, 緯度, 経度, 都道府県, 支庁, 群もしくは市, 市町村, 行政区域コード
    //    xpsj,43.154296875,140.80078125,北海道,後志支庁,余市郡,余市町,01408
    static int IDX_HASH = 0;
    static int IDX_LAT = 1;
    static int IDX_LON = 2;
    static int IDX_PREF = 3;
    static int IDX_BRANCH = 4;
    static int IDX_COUNTRY = 5;
    static int IDX_CITY = 6;
    static int IDX_CODE = 7;

    public static String convert2Pref(double lat, double lon)
    {
        String hash = GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 5);
        String[] s = GEOHASH_MAP.get(hash);
        //if null throw exception ? not japanese lat/lon exception
        return s != null ? GEOHASH_MAP.get(hash)[IDX_PREF] : "";
    }

    public static String convert2City(double lat, double lon)
    {
        String hash = GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 5);
        //if null throw exception ? not japanese lat/lon exception
        String[] s = GEOHASH_MAP.get(hash);
        if (s == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        for (int i = IDX_PREF; i < IDX_CITY; i++) {
            sb.append(s[i]);
        }

        return sb.toString();
    }

    public static String convert2GeoHash(double lat, double lon)
    {
        return GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 5);
    }

    static {
        GEOHASH_MAP = new HashMap<>();
        URL url = GeoCodeMap.class.getClassLoader().getResource(LEVEL5_CSV_PATH);
        try {
            ResultSet rs = new Csv().read(url.toString(), null, null);
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                String[] tmp = new String[meta.getColumnCount()];
                for (int i = 0; i < meta.getColumnCount(); i++) {
                    tmp[i] = Strings.nullToEmpty(rs.getString(i + 1));
                }
                GEOHASH_MAP.put(rs.getString(1), tmp);
            }
            rs.close();
        }
        catch (SQLException e) {
            throw new RuntimeException("geohash4.csv is not found!");
        }
    }
}
