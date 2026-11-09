package com.skybook.inventory.unit;

import static org.assertj.core.api.Assertions.assertThat;

import com.skybook.inventory.config.AppProperties;
import com.skybook.inventory.holds.LockStrategy;
import com.skybook.inventory.web.InternalAuthFilter;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class InternalAuthFilterTest {
    private final InternalAuthFilter filter = new InternalAuthFilter(new AppProperties("postgres://x@y/z", "secret", LockStrategy.PESSIMISTIC,
            600, 15000, 200, 3, 3, new AppProperties.Topics("b", "i", "p")));

    private MockHttpServletResponse run(MockHttpServletRequest req, MockFilterChain chain) throws Exception {
        MockHttpServletResponse res = new MockHttpServletResponse();
        filter.doFilter(req, res, chain);
        return res;
    }

    @Test
    void missingOrWrongTokenIs401() throws Exception {
        MockFilterChain chain = new MockFilterChain();
        assertThat(run(new MockHttpServletRequest("GET", "/inventory/x"), chain).getStatus()).isEqualTo(401);
        MockHttpServletRequest wrong = new MockHttpServletRequest("GET", "/inventory/x");
        wrong.addHeader("x-internal-token", "nope");
        assertThat(run(wrong, new MockFilterChain()).getStatus()).isEqualTo(401);
        assertThat(chain.getRequest()).isNull(); // never reached the controller
    }

    @Test
    void validTokenPassesAndCarriesTheUser() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/inventory/x");
        req.addHeader("x-internal-token", "secret");
        req.addHeader("x-user-id", "u1");
        req.addHeader("x-user-role", "AGENT");
        MockFilterChain chain = new MockFilterChain();
        assertThat(run(req, chain).getStatus()).isEqualTo(200);
        assertThat(chain.getRequest()).isNotNull();
        assertThat(req.getAttribute(InternalAuthFilter.USER_ATTR)).isEqualTo(new InternalAuthFilter.User("u1", "AGENT"));
    }

    @Test
    void unknownRoleMeansNoUser() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/inventory/x");
        req.addHeader("x-internal-token", "secret");
        req.addHeader("x-user-id", "u1");
        req.addHeader("x-user-role", "ADMIN");
        run(req, new MockFilterChain());
        assertThat(req.getAttribute(InternalAuthFilter.USER_ATTR)).isNull();
    }

    @Test
    void healthReadyMetricsAreOpen() throws Exception {
        for (String path : new String[] {"/health", "/ready", "/metrics"}) {
            MockFilterChain chain = new MockFilterChain();
            assertThat(run(new MockHttpServletRequest("GET", path), chain).getStatus()).isEqualTo(200);
            assertThat(chain.getRequest()).isNotNull();
        }
    }
}
