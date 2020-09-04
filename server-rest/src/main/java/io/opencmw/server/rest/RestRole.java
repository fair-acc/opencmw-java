package io.opencmw.server.rest;

import org.jetbrains.annotations.NotNull;

import io.javalin.core.security.Role;
import io.opencmw.rbac.RbacRole;

/**
 * REST specific role adapter mapping of OpenCMW's RbacRole to Javalin's Role interface description
 * @author rstein
 */
public class RestRole implements Role {
    public final RbacRole rbacRole;

    public RestRole(@NotNull final RbacRole rbacRole) {
        this.rbacRole = rbacRole;
    }

    @Override
    public String toString() {
        return rbacRole.toString();
    }
}