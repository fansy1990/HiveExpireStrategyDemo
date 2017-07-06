package common;

import java.util.*;
import java.io.*;
/**
 * Created by fansy on 2017/7/6.
 */
public class PropertiesUtil {

    public static String getProperty(String propertiesFileName, String key) {
        Properties props = new Properties();
        try {
            props.load(PropertiesUtil.class.getResourceAsStream("/" + propertiesFileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (String) props.get(key);
    }
    public static Properties getProperty(String propertiesFileName) {
        Properties props = new Properties();
        try {
            props.load(PropertiesUtil.class.getResourceAsStream("/" + propertiesFileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  props;
    }
}
