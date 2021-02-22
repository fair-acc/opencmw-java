package io.opencmw.server.rest.util;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import io.javalin.http.Context;
import io.opencmw.server.rest.RestServer;

public class MessageBundle {
    private static final String ATTR_CURRENT_MESSAGES = "msg";
    private static final String ATTR_CURRENT_USER = "currentUser";
    private static final String ATTR_CURRENT_ROLES = "currentRoles";
    private final ResourceBundle messages;

    public MessageBundle(final String languageTag) {
        final Locale locale = languageTag == null ? Locale.ENGLISH : new Locale(languageTag);
        messages = ResourceBundle.getBundle("localisation/messages", locale);
    }

    public String get(final String message) {
        try {
            return messages.getString(message);
        } catch (MissingResourceException e) {
            return message;
        }
    }

    public final String get(final String key, final Object... args) {
        return MessageFormat.format(get(key), args);
    }

    public static Map<String, Object> baseModel(Context ctx) {
        final Map<String, Object> model = new HashMap<>(); // NOPMD - thread-safe usage
        model.put(ATTR_CURRENT_MESSAGES, new MessageBundle(RestServer.getSessionLocale(ctx)));
        model.put(ATTR_CURRENT_USER, RestServer.getSessionCurrentUser(ctx));
        model.put(ATTR_CURRENT_ROLES, RestServer.getSessionCurrentRoles(ctx));
        return model;
    }
}
