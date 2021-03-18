package io.opencmw.rbac;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class RbacTokenTest {
    @Test
    void basicTests() {
        assertDoesNotThrow(() -> new RbacToken(BasicRbacRole.ADMIN, "testHash"));
        assertThrows(IllegalArgumentException.class, () -> new RbacToken(null, "testHash"));
        assertThrows(IllegalArgumentException.class, () -> new RbacToken(BasicRbacRole.ADMIN, null));
        assertThrows(IllegalArgumentException.class, () -> RbacToken.from("RBAC=ANYONE,hash,invalidHash"));
        final RbacToken testToken = new RbacToken(BasicRbacRole.ADMIN, "testHash");
        assertNotNull(testToken.toString());
        assertEquals(BasicRbacRole.ADMIN, testToken.getRole());
        assertEquals("testHash", testToken.getSignedHashCode());
        assertArrayEquals(testToken.toString().getBytes(StandardCharsets.UTF_8), testToken.getBytes());

        assertEquals(testToken, RbacToken.from(testToken.getBytes()));
        assertEquals(testToken, RbacToken.from(testToken.toString()));
        assertEquals(testToken.hashCode(), RbacToken.from(testToken.getBytes()).hashCode());
        assertEquals("RBAC=ANYONE,", RbacToken.from((String) null).toString());
        assertEquals("RBAC=ANYONE,", RbacToken.from("").toString());
        assertEquals(testToken, testToken, "reference identity");
        assertNotEquals(testToken, new Object(), "non conforming object"); // NOPMD
        assertNotEquals(testToken, new RbacToken(BasicRbacRole.ADMIN, "otherHash"), "different hash");
        assertNotEquals(testToken.hashCode(), new RbacToken(BasicRbacRole.ADMIN, "otherHash").hashCode(), "different hash");
        assertNotEquals(testToken, new RbacToken(BasicRbacRole.NULL, "testHash"), "different role");
        assertNotEquals(testToken.hashCode(), new RbacToken(BasicRbacRole.NULL, "testHash").hashCode(), "different role");
    }
}