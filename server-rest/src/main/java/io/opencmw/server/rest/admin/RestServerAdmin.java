package io.opencmw.server.rest.admin;

import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.post;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.rbac.RbacRole;
import io.opencmw.server.rest.RestRole;
import io.opencmw.server.rest.RestServer;
import io.opencmw.server.rest.login.LoginController;
import io.opencmw.server.rest.user.RestUserHandler;
import io.opencmw.server.rest.util.MessageBundle;

/**
 * Basic ResetServer admin interface
 * @author rstein
 */
@SuppressWarnings("PMD.FieldNamingConventions")
public class RestServerAdmin { // NOPMD - nomen est omen
    private static final Logger LOGGER = LoggerFactory.getLogger(RestServerAdmin.class);
    private static final String ENDPOINT_ADMIN = "/admin";
    private static final String TEMPLATE_ADMIN = "/velocity/admin/admin.vm";

    @OpenApi(
            description = "endpoint to receive admin requests",
            operationId = "serveAdminPage",
            summary = "serve ",
            tags = { "RestServerAdmin" },
            responses = {
                @OpenApiResponse(status = "200", content = @OpenApiContent(type = "text/html"))
            })
    private static final Handler serveAdminPage
            = ctx -> {
        final String userName = LoginController.getSessionCurrentUser(ctx);
        final Set<RbacRole> roles = LoginController.getSessionCurrentRoles(ctx);
        if (!roles.contains(BasicRbacRole.ADMIN)) {
            LOGGER.atWarn().addArgument(userName).log("user '{}' does not have the required admin access rights");
            ctx.status(401).result("admin access denied");
            return;
        }
        RestUserHandler userHandler = RestServer.getUserHandler();
        final Map<String, Object> model = MessageBundle.baseModel(ctx);
        model.put("userHandler", userHandler);
        model.put("users", userHandler.getAllUserNames());
        model.put("endpoints", RestServer.getEndpoints());

        ctx.render(TEMPLATE_ADMIN, model);
    };

    @OpenApi(
            description = "endpoint to receive admin requests",
            operationId = "handleAdminPost",
            summary = "POST ",
            tags = { "RestServerAdmin" },
            responses = {
                @OpenApiResponse(status = "200", content = @OpenApiContent(type = "text/html"))
            })
    private static final Handler handleAdminPost
            = ctx -> {
        final String userName = LoginController.getSessionCurrentUser(ctx);
        final Set<RbacRole> roles = LoginController.getSessionCurrentRoles(ctx);
        if (!roles.contains(BasicRbacRole.ADMIN)) {
            LOGGER.atWarn().addArgument(userName).log("user '{}' does not have the required admin access rights");
            ctx.status(401).result("admin access denied");
            return;
        }
        final Map<String, Object> model = MessageBundle.baseModel(ctx);

        // parse and process admin stuff
        ctx.render(TEMPLATE_ADMIN, model);
    };

    /**
     * registers the login/logout and locale change listener
     */
    public static void register() {
        RestServer.getInstance().routes(() -> {
            post(ENDPOINT_ADMIN, handleAdminPost, Collections.singleton(new RestRole(BasicRbacRole.ADMIN)));
            get(ENDPOINT_ADMIN, serveAdminPage, Collections.singleton(new RestRole(BasicRbacRole.ADMIN)));
        });
    }
}
