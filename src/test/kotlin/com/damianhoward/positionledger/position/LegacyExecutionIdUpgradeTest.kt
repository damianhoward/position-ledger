package com.damianhoward.positionledger.position

import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

/**
 * V5 applied to the shape production held: some fills recorded before execution ids existed, with
 * a null exec_id, and later ones with the producer's id. The live ledger had ten of the first and
 * sixteen of the second, and is the one database here whose contents cannot be recovered, so what
 * the migration does to rows already present is proven against rows, not against an empty schema.
 */
@Testcontainers(disabledWithoutDocker = true)
class LegacyExecutionIdUpgradeTest {
    companion object {
        private val V4 = MigrationVersion.fromVersion("4")

        @Container
        @JvmField
        val oracle: org.testcontainers.oracle.OracleContainer =
            org.testcontainers.oracle
                .OracleContainer("gvenzl/oracle-free:23-slim-faststart")
                .withStartupTimeout(java.time.Duration.ofMinutes(3))
    }

    private fun connect(): Connection = DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password)

    private fun flyway(target: MigrationVersion?) =
        Flyway
            .configure()
            .dataSource(oracle.jdbcUrl, oracle.username, oracle.password)
            .cleanDisabled(false)
            .apply { target?.let { target(it) } }
            .load()

    private fun insert(
        offset: Long,
        execId: String?,
    ) = connect().use { connection ->
        connection
            .prepareStatement(
                "INSERT INTO fills (source_topic, source_partition, source_offset, symbol, price, signed_size, " +
                    "maker_order_id, taker_order_id, time_millis, exec_id) VALUES ('orderbook.fills', 0, ?, 'SIM', 101.5, 5, 1, 2, 1720620000000, ?)",
            ).use { statement ->
                statement.setLong(1, offset)
                statement.setString(2, execId)
                statement.executeUpdate()
            }
    }

    private fun execIds(): List<String?> =
        connect().use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT exec_id FROM fills ORDER BY source_offset").use { rows ->
                    buildList { while (rows.next()) add(rows.getString(1)) }
                }
            }
        }

    /** At V4, with production's mix of rows, asserted rather than assumed. */
    private fun startFromV4() {
        flyway(V4).apply { clean() }.migrate()
        assertEquals(V4, flyway(null).info().current().version, "setup did not stop at V4")
        insert(0, null)
        insert(1, null)
        insert(2, "1720620000000-7")
        assertEquals(listOf(null, null, "1720620000000-7"), execIds())
    }

    @Test
    fun `fills without an execution id take one from the coordinates they were recorded at`() {
        startFromV4()
        flyway(null).migrate()
        assertEquals(listOf("legacy-orderbook.fills-0-0", "legacy-orderbook.fills-0-1", "1720620000000-7"), execIds())
    }

    @Test
    fun `an execution id the producer stamped is left as it was`() {
        startFromV4()
        flyway(null).migrate()
        assertEquals("1720620000000-7", execIds().last())
    }

    @Test
    fun `no fill can be recorded without an execution id afterwards`() {
        startFromV4()
        flyway(null).migrate()
        assertThrows(SQLException::class.java) { insert(3, null) }
    }

    @Test
    fun `the upgrade lands where a clean install does`() {
        startFromV4()
        flyway(null).migrate()
        val upgraded = flyway(null).info().current().version
        flyway(null).apply { clean() }.migrate()
        assertEquals(flyway(null).info().current().version, upgraded)
    }
}
