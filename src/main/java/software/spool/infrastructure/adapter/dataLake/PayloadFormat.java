package software.spool.infrastructure.adapter.dataLake;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.MediaType;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * How the data lake keeps a payload as it came, from its media type: the extension of its file, and the value a
 * reader hands on. A JSON payload, including any {@code +json} media type, is read as an object; any other is read
 * as its bytes.
 */
public final class PayloadFormat {
    private static final String TYPE_SEPARATOR = "/";
    private static final String PARAMETER_SEPARATOR = ";";
    private static final String JSON = "json";
    private static final String JSON_SUFFIX = "+json";
    private static final String UNKNOWN_EXTENSION = "bin";
    private static final Pattern EXTENSION = Pattern.compile("[a-z0-9][a-z0-9.-]*");

    private PayloadFormat() {
    }

    public static String extensionOf(MediaType mediaType) {
        if (isJson(mediaType)) return JSON;
        String subtype = subtypeOf(mediaType);
        return EXTENSION.matcher(subtype).matches() ? subtype : UNKNOWN_EXTENSION;
    }

    public static boolean isJson(MediaType mediaType) {
        String subtype = subtypeOf(mediaType);
        return subtype.equals(JSON) || subtype.endsWith(JSON_SUFFIX);
    }

    public static Object readable(MediaType mediaType, byte[] payload) {
        return isJson(mediaType) ? PayloadDeserializerFactory.json().asMap().deserialize(payload) : payload;
    }

    private static String subtypeOf(MediaType mediaType) {
        String type = mediaType.value().split(PARAMETER_SEPARATOR, 2)[0].strip().toLowerCase(Locale.ROOT);
        int separator = type.indexOf(TYPE_SEPARATOR);
        return separator < 0 ? "" : type.substring(separator + 1);
    }
}
