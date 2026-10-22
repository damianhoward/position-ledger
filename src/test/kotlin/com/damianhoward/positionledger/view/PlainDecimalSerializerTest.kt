package com.damianhoward.positionledger.view

import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class PlainDecimalSerializerTest {
    @Test
    fun `writes plain notation and reads the same decimal back, scale included`() {
        // toString would write 1E+3 for this value; the contract is plain notation.
        assertEquals("1000", Json.encodeToString(PlainDecimalSerializer, BigDecimal("1E+3")))
        assertEquals(BigDecimal("1000.00"), Json.decodeFromString(PlainDecimalSerializer, "1000.00"))
    }
}
