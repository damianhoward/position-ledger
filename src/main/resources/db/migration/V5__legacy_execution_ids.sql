-- Every fill gets an execution identity, and none can be recorded without one.
--
-- Fills recorded before the producer stamped an execId were deduplicated by their stream
-- coordinates alone, which a copy at new coordinates defeats: the recovery drill exported the
-- ledger and delivered every fill a second time, and the ten without an id applied twice. Each now
-- takes an id built from the coordinates it was first recorded at. That is the identity it was
-- already deduplicated by, made portable: a copy elsewhere carries it, and the unique index
-- recognises the copy. The `legacy-` prefix cannot collide with the producer's
-- `<epochMillis>-<sequence>`, which always starts with a digit.
--
-- The consumer derives the same id, from the same coordinates, for any record that arrives without
-- one, so the column can be NOT NULL from here on. Widened first, so a longer topic name cannot make
-- a derived id overflow it.
ALTER TABLE fills MODIFY (exec_id VARCHAR2(128 CHAR));
UPDATE fills
   SET exec_id = 'legacy-' || source_topic || '-' || source_partition || '-' || source_offset
 WHERE exec_id IS NULL;
ALTER TABLE fills MODIFY (exec_id NOT NULL);
