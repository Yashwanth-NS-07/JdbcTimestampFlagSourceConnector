package com.experiment.util;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TestVersion {
    @Test
    public void testVersion() {
        assertNotNull(Version.getVersion());
        System.out.println(Version.getVersion());
    }
}
