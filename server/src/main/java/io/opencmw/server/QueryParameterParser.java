package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.AbstractMap.SimpleImmutableEntry;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;

import io.opencmw.MimeType;
import io.opencmw.filter.TimingCtx;
import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.spi.ClassFieldDescription;
import io.opencmw.serialiser.utils.ClassUtils;

/**
 * Parses query parameters into PoJo structure.
 *
 *  Follows URI syntax, ie. '<pre>scheme:[//authority]path[?query][#fragment]</pre>'
 *  see <a href="https://tools.ietf.org/html/rfc3986">documentation</a>
 *
 * @author rstein
 */
public final class QueryParameterParser { // NOPMD - nomen est omen
    public static final String MIME_TYPE_TAG = "contentType";
    public static final ConcurrentMap<Type, TriConsumer> STRING_TO_CLASS_CONVERTER = new ConcurrentHashMap<>(); // NOSONAR NOPMD
    public static final ConcurrentMap<Type, BiFunction<Object, ClassFieldDescription, String>> CLASS_TO_STRING_CONVERTER = new ConcurrentHashMap<>(); // NOSONAR NOPMD
    public static final ConcurrentMap<Type, BiFunction<Object, ClassFieldDescription, Object>> CLASS_TO_OBJECT_CONVERTER = new ConcurrentHashMap<>(); // NOSONAR NOPMD
    static {
        STRING_TO_CLASS_CONVERTER.put(boolean.class, (str, obj, field) -> field.getField().setBoolean(obj, Boolean.parseBoolean(str)));
        STRING_TO_CLASS_CONVERTER.put(byte.class, (str, obj, field) -> field.getField().setByte(obj, Byte.parseByte(str)));
        STRING_TO_CLASS_CONVERTER.put(short.class, (str, obj, field) -> field.getField().setShort(obj, Short.parseShort(str)));
        STRING_TO_CLASS_CONVERTER.put(int.class, (str, obj, field) -> field.getField().setInt(obj, Integer.parseInt(str)));
        STRING_TO_CLASS_CONVERTER.put(long.class, (str, obj, field) -> field.getField().setLong(obj, Long.parseLong(str)));
        STRING_TO_CLASS_CONVERTER.put(float.class, (str, obj, field) -> field.getField().setFloat(obj, Float.parseFloat(str)));
        STRING_TO_CLASS_CONVERTER.put(double.class, (str, obj, field) -> field.getField().setDouble(obj, Double.parseDouble(str)));
        STRING_TO_CLASS_CONVERTER.put(Boolean.class, (str, obj, field) -> field.getField().set(obj, Boolean.parseBoolean(str)));
        STRING_TO_CLASS_CONVERTER.put(Byte.class, (str, obj, field) -> field.getField().set(obj, Byte.parseByte(str)));
        STRING_TO_CLASS_CONVERTER.put(Short.class, (str, obj, field) -> field.getField().set(obj, Short.parseShort(str)));
        STRING_TO_CLASS_CONVERTER.put(Integer.class, (str, obj, field) -> field.getField().set(obj, Integer.parseInt(str)));
        STRING_TO_CLASS_CONVERTER.put(Long.class, (str, obj, field) -> field.getField().set(obj, Long.parseLong(str)));
        STRING_TO_CLASS_CONVERTER.put(Float.class, (str, obj, field) -> field.getField().set(obj, Float.parseFloat(str)));
        STRING_TO_CLASS_CONVERTER.put(Double.class, (str, obj, field) -> field.getField().set(obj, Double.parseDouble(str)));
        STRING_TO_CLASS_CONVERTER.put(String.class, (str, obj, field) -> field.getField().set(obj, str));

        final BiFunction<Object, ClassFieldDescription, String> objToString = (obj, field) -> {
            final Object ret = field.getField().get(obj);
            return ret == null || ret.getClass().equals(Object.class) ? "" : ret.toString();
        };
        CLASS_TO_STRING_CONVERTER.put(boolean.class, (obj, field) -> Boolean.toString(field.getField().getBoolean(obj)));
        CLASS_TO_STRING_CONVERTER.put(byte.class, (obj, field) -> Byte.toString(field.getField().getByte(obj)));
        CLASS_TO_STRING_CONVERTER.put(short.class, (obj, field) -> Short.toString(field.getField().getShort(obj)));
        CLASS_TO_STRING_CONVERTER.put(int.class, (obj, field) -> Integer.toString(field.getField().getInt(obj)));
        CLASS_TO_STRING_CONVERTER.put(long.class, (obj, field) -> Long.toString(field.getField().getLong(obj)));
        CLASS_TO_STRING_CONVERTER.put(float.class, (obj, field) -> Float.toString(field.getField().getFloat(obj)));
        CLASS_TO_STRING_CONVERTER.put(double.class, (obj, field) -> Double.toString(field.getField().getDouble(obj)));
        CLASS_TO_STRING_CONVERTER.put(boolean[].class, (obj, field) -> Arrays.toString((boolean[]) field.getField().get(obj)));
        CLASS_TO_STRING_CONVERTER.put(byte[].class, (obj, field) -> Arrays.toString((byte[]) field.getField().get(obj)));
        CLASS_TO_STRING_CONVERTER.put(short[].class, (obj, field) -> Arrays.toString((short[]) field.getField().get(obj)));
        CLASS_TO_STRING_CONVERTER.put(int[].class, (obj, field) -> Arrays.toString((int[]) field.getField().get(obj)));
        CLASS_TO_STRING_CONVERTER.put(long[].class, (obj, field) -> Arrays.toString((long[]) field.getField().get(obj)));
        CLASS_TO_STRING_CONVERTER.put(float[].class, (obj, field) -> Arrays.toString((float[]) field.getField().get(obj)));
        CLASS_TO_STRING_CONVERTER.put(double[].class, (obj, field) -> Arrays.toString((double[]) field.getField().get(obj)));
        CLASS_TO_STRING_CONVERTER.put(Boolean.class, objToString);
        CLASS_TO_STRING_CONVERTER.put(Byte.class, objToString);
        CLASS_TO_STRING_CONVERTER.put(Short.class, objToString);
        CLASS_TO_STRING_CONVERTER.put(Integer.class, objToString);
        CLASS_TO_STRING_CONVERTER.put(Long.class, objToString);
        CLASS_TO_STRING_CONVERTER.put(Float.class, objToString);
        CLASS_TO_STRING_CONVERTER.put(Double.class, objToString);
        CLASS_TO_STRING_CONVERTER.put(String.class, (obj, field) -> Objects.requireNonNullElse(field.getField().get(obj),"").toString());

        CLASS_TO_OBJECT_CONVERTER.put(boolean.class, (obj, field) -> field.getField().getBoolean(obj));
        CLASS_TO_OBJECT_CONVERTER.put(byte.class, (obj, field) -> field.getField().getByte(obj));
        CLASS_TO_OBJECT_CONVERTER.put(short.class, (obj, field) -> field.getField().getShort(obj));
        CLASS_TO_OBJECT_CONVERTER.put(int.class, (obj, field) -> field.getField().getInt(obj));
        CLASS_TO_OBJECT_CONVERTER.put(long.class, (obj, field) -> field.getField().getLong(obj));
        CLASS_TO_OBJECT_CONVERTER.put(float.class, (obj, field) -> field.getField().getFloat(obj));
        CLASS_TO_OBJECT_CONVERTER.put(double.class, (obj, field) -> field.getField().getDouble(obj));
        CLASS_TO_OBJECT_CONVERTER.put(Object.class, (obj, field) -> field.getField().get(obj));

        // special known objects
        STRING_TO_CLASS_CONVERTER.put(Object.class, (str, obj, field) -> field.getField().set(obj, new Object()));
        STRING_TO_CLASS_CONVERTER.put(MimeType.class, (str, obj, field) -> field.getField().set(obj, MimeType.getEnum(str)));
        STRING_TO_CLASS_CONVERTER.put(TimingCtx.class, (str, obj, field) -> field.getField().set(obj, TimingCtx.get(str)));

        CLASS_TO_STRING_CONVERTER.put(Object.class, objToString);
        CLASS_TO_STRING_CONVERTER.put(MimeType.class, (obj, field) -> {
            final Object ret = field.getField().get(obj);
            return ret == null || ret.getClass().equals(Object.class) ? "" : ((MimeType) ret).name();
        });
        CLASS_TO_STRING_CONVERTER.put(TimingCtx.class, (obj, field) -> {
            final Object ctx = field.getField().get(obj);
            return ctx instanceof TimingCtx ? ((TimingCtx) ctx).selector : "";
        });
    }

    private QueryParameterParser() {
        // this is a utility class
    }

    public static URI appendQueryParameter(URI oldUri, String appendQuery) throws URISyntaxException {
        if (appendQuery == null || appendQuery.isBlank()) {
            return oldUri;
        }
        return new URI(oldUri.getScheme(), oldUri.getAuthority(), oldUri.getPath(), oldUri.getQuery() == null ? appendQuery : (oldUri.getQuery() + "&" + appendQuery), oldUri.getFragment());
    }

    /**
     *
     * @param queryParameterMap query parameter map
     * @return queryString a <a href="https://tools.ietf.org/html/rfc3986">rfc3986</a> query parameter string
     */
    @SuppressWarnings("PMD")
    public static String generateQueryParameter(final Map<String, ?> queryParameterMap) { //NOSONAR - complexity justified
        final StringBuilder builder = new StringBuilder();

        final Set<? extends Map.Entry<String, ?>> entrySet = queryParameterMap.entrySet();
        final Iterator<? extends Map.Entry<String, ?>> iterator = entrySet.iterator();
        boolean first = true;
        while (iterator.hasNext()) {
            Map.Entry<String, ?> item = iterator.next();
            String key = item.getKey();
            Object values = item.getValue();
            if (!first) {
                builder.append('&');
            }
            if (values == null) {
                builder.append(key);
            } else if (List.class.isAssignableFrom(values.getClass())) {
                @SuppressWarnings("unchecked") // checked with above isAssignableFrom
                List<Object> list = (List<Object>) values;
                for (Object val : list) {
                    if (!first) {
                        builder.append('&');
                    }
                    if (val == null) {
                        builder.append(key);
                    } else {
                        builder.append(key).append('=').append(val);
                    }
                    first = false;
                }
            } else {
                // non list object
                builder.append(key).append('=').append(values);
            }
            first = false;
        }
        return builder.toString();
    }

    /**
     *
     * @param obj storage class
     * @return queryString a <a href="https://tools.ietf.org/html/rfc3986">rfc3986</a> query parameter string
     */
    public static String generateQueryParameter(Object obj) {
        final ClassFieldDescription fieldDescription = ClassUtils.getFieldDescription(obj.getClass());
        final StringBuilder builder = new StringBuilder();
        final List<FieldDescription> children = fieldDescription.getChildren();
        for (int index = 0; index < children.size(); index++) {
            ClassFieldDescription field = (ClassFieldDescription) children.get(index);
            final BiFunction<Object, ClassFieldDescription, String> mapFunction = CLASS_TO_STRING_CONVERTER.get(field.getType());
            final String str;
            if (mapFunction == null) {
                str = CLASS_TO_STRING_CONVERTER.get(Object.class).apply(obj, field);
            } else {
                str = mapFunction.apply(obj, field);
            }
            builder.append(field.getFieldName()).append('=').append(str == null ? "" : URLEncoder.encode(str, UTF_8));
            if (index != children.size() - 1) {
                builder.append('&');
            }
        }
        return builder.toString();
    }

    public static Map<String, List<String>> getMap(final String queryParam) {
        if (queryParam == null || queryParam.isBlank()) {
            return Collections.emptyMap();
        }

        return Arrays.stream(StringUtils.split(queryParam, "&;"))
                .map(QueryParameterParser::splitQueryParameter)
                .collect(Collectors.groupingBy(SimpleImmutableEntry::getKey, HashMap::new, mapping(Map.Entry::getValue, toList())));
    }

    public static @NotNull MimeType getMimeType(final String queryString) {
        final List<String> mimeTypeList = QueryParameterParser.getMap(queryString).get(MIME_TYPE_TAG);
        return mimeTypeList == null || mimeTypeList.isEmpty() ? MimeType.UNKNOWN : MimeType.getEnum(mimeTypeList.get(mimeTypeList.size() - 1));
    }

    /**
     * Parse query parameter t.
     *
     * @param <T>         generic storage class type to be returned
     * @param clazz       storage class type
     * @param queryString a <a href="https://tools.ietf.org/html/rfc3986">rfc3986</a> query parameter string
     * @return PoJo with those parameters that could be matched (N.B. flat map only)
     * @throws NoSuchMethodException     in case the class does not have a accessible constructor
     * @throws IllegalAccessException    in case the class cannot be instantiated
     * @throws InvocationTargetException in case the class cannot be instantiated
     * @throws InstantiationException    in case the class cannot be instantiated
     */
    public static <T> T parseQueryParameter(Class<T> clazz, final String queryString) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        final ClassFieldDescription fieldDescription = ClassUtils.getFieldDescription(clazz);
        final Constructor<T> constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true); // NOSONAR NOPMD
        final T obj = constructor.newInstance();
        final Map<String, List<String>> queryMap = getMap(queryString);
        for (FieldDescription f : fieldDescription.getChildren()) {
            ClassFieldDescription field = (ClassFieldDescription) f;
            final List<String> values = queryMap.get(field.getFieldName());
            final TriConsumer mapFunction = STRING_TO_CLASS_CONVERTER.get(field.getType());
            if (mapFunction == null || values == null || values.isEmpty()) {
                // skip field
                continue;
            }
            final String value = values.get(values.size() - 1);
            try {
                mapFunction.accept(value, obj, field);
            } catch (final Exception e) { // NOPMD exception is being rethrown
                throw new IllegalArgumentException("error parsing value '" + value + "' for field: '" + clazz.getName() + "::" + field.getFieldName() + "'", e);
            }
        }
        return obj;
    }

    public static URI removeQueryParameter(URI oldUri, String removeQuery) throws URISyntaxException {
        if (removeQuery == null || removeQuery.isBlank() || oldUri.getQuery() == null) {
            return oldUri;
        }
        final Map<String, List<String>> query = getMap(oldUri.getQuery());
        final int idx = removeQuery.indexOf('=');
        if (idx >= 0) {
            final String key = idx > 0 ? removeQuery.substring(0, idx) : removeQuery;
            final String value = idx > 0 && removeQuery.length() > idx + 1 ? removeQuery.substring(idx + 1) : null;
            final List<String> entry = query.get(key);
            if (entry != null) {
                entry.remove(value);
                if (entry.isEmpty()) {
                    query.remove(value);
                }
            }
        } else {
            query.remove(removeQuery);
        }
        final String newQueryParameter = QueryParameterParser.generateQueryParameter(query);
        return new URI(oldUri.getScheme(), oldUri.getAuthority(), oldUri.getPath(), newQueryParameter, oldUri.getFragment());
    }

    /**
     * used as lambda expression for user-level code to read/write data into the query pojo
     *
     * @author rstein
     */
    public interface TriConsumer {
        /**
         * Performs this operation on the given arguments.
         *
         * @param str the reference string
         * @param rootObj the specific root object reference the given field is part of
         * @param field the description for the given class member, if null then rootObj is written/read directly
         */
        void accept(String str, Object rootObj, ClassFieldDescription field);
    }

    @SuppressWarnings("PMD.DefaultPackage")
    static SimpleImmutableEntry<String, String> splitQueryParameter(String queryParameter) { // NOPMD package private for unit-testing purposes
        final int idx = queryParameter.indexOf('=');
        final String key = idx > 0 ? queryParameter.substring(0, idx) : queryParameter;
        final String value = idx > 0 && queryParameter.length() > idx + 1 ? queryParameter.substring(idx + 1) : null;
        return new SimpleImmutableEntry<>(URLDecoder.decode(key, UTF_8), value == null ? null : URLDecoder.decode(value, UTF_8));
    }
}
