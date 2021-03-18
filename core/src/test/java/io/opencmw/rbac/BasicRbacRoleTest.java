package io.opencmw.rbac;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class BasicRbacRoleTest {
    @Test
    void testBasicRbac() {
        assertTrue(BasicRbacRole.NULL.compareTo(BasicRbacRole.ADMIN) > 0, "ADMIN higher than NULL");
        assertTrue(BasicRbacRole.ADMIN.compareTo(BasicRbacRole.NULL) < 0, "ADMIN higher than NULL");
        for (RbacRole<BasicRbacRole> role : BasicRbacRole.values()) {
            assertNotNull(role.toString());
            assertNotNull(role.getName());
            assertTrue(role.getPriority() >= 0, "positive role priority");
            final BasicRbacRole roleIdentity = BasicRbacRole.NULL.getRole(role.getName());
            assertEquals(role, roleIdentity, "role identity for " + role);

            assertTrue(role.compareTo(BasicRbacRole.NULL) <= 0, "null having the lowest priority - role: " + role);
        }

        assertEquals(BasicRbacRole.values().length, BasicRbacRole.NULL.getRoles(Stream.of(BasicRbacRole.values()).map(BasicRbacRole::getName).collect(Collectors.joining(","))).size(), "role string identity #1");
        final String roleString = BasicRbacRole.NULL.getRoles(Stream.of(BasicRbacRole.values()).collect(Collectors.toSet()));
        assertNotNull(roleString);
        assertEquals(BasicRbacRole.values().length, BasicRbacRole.NULL.getRoles(roleString).size(), "role string identity #2");
        assertEquals(0, BasicRbacRole.NULL.getRoles("*").size());
        assertThrows(IllegalArgumentException.class, () -> BasicRbacRole.NULL.getRoles(":"), "invalid characters");
    }
}