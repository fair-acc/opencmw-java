package io.opencmw.server.rest.user;

import java.util.Collections;
import java.util.Set;

import io.opencmw.rbac.RbacRole;

public class RestUser {
    protected final String userName;
    protected String salt;
    protected String hashedPassword;
    private final Set<RbacRole> roles;

    public RestUser(final String username, final String salt, final String hashedPassword, final Set<RbacRole> roles) {
        this.userName = username;
        this.salt = salt;
        this.hashedPassword = hashedPassword;
        this.roles = roles == null ? Collections.emptySet() : Collections.unmodifiableSet(roles);
    }

    protected Set<RbacRole> getRoles() {
        return roles;
    }

    @Override
    public String toString() {
        return "RestUser{" + userName + ", roles=" + roles + "}";
    }
}
