package io.opencmw.server.rest;

import static io.opencmw.rbac.BasicRbacRole.ANYONE;

import java.util.Collections;
import java.util.Set;

import io.javalin.core.security.Role;
import io.javalin.plugin.openapi.dsl.OpenApiBuilder;
import io.javalin.plugin.openapi.dsl.OpenApiDocumentation;
import io.opencmw.server.rest.user.RestUser;

public class RestServerTests {
    public static void main(String[] argv) {
        Set<Role> accessRoles = Collections.singleton(new RestRole(ANYONE));

        OpenApiDocumentation apiDocumentation = OpenApiBuilder.document().body(RestUser.class).json("200", RestUser.class);
        RestServer.getInstance().get("/", OpenApiBuilder.documented(apiDocumentation, ctx -> {
            ctx.html("Hello World!");
        }), accessRoles);
        RestServer.getInstance().get("/helloWorld", OpenApiBuilder.documented(apiDocumentation, ctx -> {
            ctx.html("Hello World!");
        }), accessRoles);
    }
}
