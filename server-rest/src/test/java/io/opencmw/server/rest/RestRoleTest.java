package io.opencmw.server.rest;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.opencmw.rbac.BasicRbacRole;

class RestRoleTest {
    @Test
    void basicTest() {
        assertDoesNotThrow(() -> new RestRole(BasicRbacRole.ADMIN));

        assertNotNull(new RestRole(BasicRbacRole.ADMIN).toString());
    }
}