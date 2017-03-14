package org.davidmoten.bigsort2;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class UtilTest {
    
    @Test
    public void isUtility() {
        Asserts.assertIsUtilityClass(Util.class);
    }

}
