package orderbook.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    private static final Properties props = new Properties();

    static {
        try(InputStream input = Config.class.getClassLoader().getResourceAsStream("config.properties")){
            if(input!=null){
                props.load(input);
            }else{
                System.err.println("Config file not found! Using defaults");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean getBoolean(String key, boolean defaultValue){
        String value = props.getProperty(key);
        if(value == null) return defaultValue;
        return Boolean.parseBoolean(value);
    }

    public static boolean getBoolean(String key){
        String value = props.getProperty(key);
        if(value == null){
            throw new IllegalArgumentException("Missing required config: " + key);
        }
        return Boolean.parseBoolean(value);
    }

    public static String getString(String key){
        return props.getProperty(key);
    }

    public static int getInt(String key){
        return Integer.parseInt(props.getProperty(key));
    }
}
