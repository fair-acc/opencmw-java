package io.opencmw.rbac;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import org.zeromq.ZMQ;

public class RbacToken {
    private final static String RBAC_TOKEN_PREFIX = "RBAC";
    private final String signedHashCode;
    private final RbacRole<? extends RbacRole<?>> rbacRole;
    private final String stringRepresentation;
    private final byte[] byteRepresentation;

    public RbacToken(final RbacRole<? extends RbacRole<?>> rbacRole, final String signedHashCode) {
        if (rbacRole == null) {
            throw new IllegalArgumentException("rbacRole must not be null: " + null);
        }
        if (signedHashCode == null) {
            throw new IllegalArgumentException("signedHashCode must not be null: " + null);
        }
        this.rbacRole = rbacRole;
        this.signedHashCode = signedHashCode;
        this.stringRepresentation = RBAC_TOKEN_PREFIX + "=" + this.rbacRole.getName() + "," + signedHashCode;
        this.byteRepresentation = stringRepresentation.getBytes(StandardCharsets.UTF_8);

        // BCrypt.hashpw()
    }

    public RbacRole<? extends RbacRole<?>> getRole() {
        return rbacRole;
    }

    public String getSignedHashCode() {
        return signedHashCode;
    }

    @Override
    public String toString() {
        return stringRepresentation;
    }

    public byte[] getBytes() {
        return Arrays.copyOf(byteRepresentation, byteRepresentation.length);
    }

    public static RbacToken from(final byte[] rbacToken) {
        return from(rbacToken, rbacToken.length);
    }

    public static RbacToken from(final byte[] rbacToken, final int length) {
        return from(new String(rbacToken, 0, length, ZMQ.CHARSET));
    }

    public static RbacToken from(final String rbacToken) {
        if (rbacToken == null || rbacToken.isBlank()) {
            return new RbacToken(BasicRbacRole.ANYONE, "");
        }
        final String[] component = rbacToken.split("[,=]");
        if (component.length != 3 || !RBAC_TOKEN_PREFIX.equals(component[0])) {
            throw new IllegalArgumentException("RBAC token error: sent token with less or more than two delimiter: " + component.length + " token=" + rbacToken);
        }
        return new RbacToken(BasicRbacRole.NULL.getRole(component[1]), component[2]);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RbacToken)) {
            return false;
        }
        final RbacToken rbacToken = (RbacToken) o;

        if (!signedHashCode.equals(rbacToken.signedHashCode)) {
            return false;
        }
        return rbacRole.equals(rbacToken.rbacRole);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rbacRole, signedHashCode);
    }
}
