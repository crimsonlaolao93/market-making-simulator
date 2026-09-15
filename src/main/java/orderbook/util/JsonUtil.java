package orderbook.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;

public class JsonUtil {

    private static final ObjectMapper jackson = new ObjectMapper();

    public static String toJson(Object obj){
        try {
            return jackson.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new JSONException("Jackson serialization failed", e);
        }
    }
}
