package io.opencmw.rbac;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Interface for Role-Based-Access-Control (RBAC) roles
 *
 * original RBAC concept:
 * <ul>
 *  <li> Ferraiolo, D.F. &amp; Kuhn, D.R. (October 1992). "Role-Based Access Control". 15th National Computer Security Conference: 554–563.
 *      https://csrc.nist.gov/CSRC/media/Publications/conference-paper/1992/10/13/role-based-access-controls/documents/ferraiolo-kuhn-92.pdf
 *  </li>
 *  <li> Sandhu, R., Coyne, E.J., Feinstein, H.L. and Youman, C.E. (August 1996). "Role-Based Access Control Models". IEEE Computer. 29 (2): 38–47. CiteSeerX 10.1.1.50.7649. doi:10.1109/2.485845
 *      https://csrc.nist.gov/projects/role-based-access-control
 *  </li>
 * </ul>
 */
public interface RbacRole<T extends RbacRole<T>> extends Comparable<T> {
    default String getRoles(final Set<T> roleSet) {
        return roleSet.stream().map(RbacRole::toString).collect(Collectors.joining(", "));
    }

    default Set<T> getRoles(final String roleString) {
        if (roleString.contains(":")) {
            throw new IllegalArgumentException("roleString must not contain [:]");
        }

        final HashSet<T> roles = new HashSet<>();
        for (final String role : roleString.replaceAll("\\s", "").split(",")) {
            if (role == null || role.isEmpty() || "*".equals(role)) { // NOPMD
                continue;
            }
            roles.add(getRole(role.toUpperCase(Locale.UK)));
        }

        return Collections.unmodifiableSet(roles);
    }

    T getRole(String roleName);

    /**
     *
     * @return role name
     */
    String getName();

    /**
     *
     * @return role priority used to schedule tasks or position in queues ( smaller numbers == higher importance)
     */
    int getPriority();

    @Override
    default int compareTo(@NotNull RbacRole otherRole) {
        System.err.println("T = " + otherRole);
        if (getPriority() > otherRole.getPriority()) {
            return 1;
        }
        if (getPriority() == otherRole.getPriority()) {
            return 0;
        }
        return 1;
    }
}
