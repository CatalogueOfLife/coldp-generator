package org.catalogueoflife.data.colac;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class OldSchemaReaderTest {

  /**
   * Tree: kingdom(1) ← genus(2) ← species(3, NOT accepted) ← variety(4).
   * The 2005–2010 ITIS data attaches accepted infraspecies under a species node that is itself a
   * synonym (is_accepted_name=0) and therefore never emitted; the variety's parentID must be
   * redirected to the nearest accepted ancestor.
   */
  @Test
  public void testAcceptedAncestor() {
    Int2IntOpenHashMap childParent = new Int2IntOpenHashMap();
    childParent.defaultReturnValue(0);
    childParent.put(2, 1); // genus -> kingdom
    childParent.put(3, 2); // species(synonym) -> genus
    childParent.put(4, 3); // variety -> species(synonym)
    IntOpenHashSet accepted = new IntOpenHashSet(new int[]{1, 2, 4});

    // a non-accepted parent resolves to the nearest accepted ancestor (the genus)
    assertEquals(2, OldSchemaReader.acceptedAncestor(3, childParent, accepted));
    // an already-accepted parent is returned unchanged
    assertEquals(2, OldSchemaReader.acceptedAncestor(2, childParent, accepted));
    assertEquals(1, OldSchemaReader.acceptedAncestor(1, childParent, accepted));
  }

  /** Several consecutive non-accepted levels are skipped up to the accepted kingdom root. */
  @Test
  public void testAcceptedAncestorSkipsMultipleLevels() {
    Int2IntOpenHashMap childParent = new Int2IntOpenHashMap();
    childParent.defaultReturnValue(0);
    childParent.put(11, 10); // genus(not accepted) -> kingdom
    childParent.put(12, 11); // species(not accepted) -> genus(not accepted)
    childParent.put(13, 12); // variety -> species(not accepted)
    IntOpenHashSet accepted = new IntOpenHashSet(new int[]{10, 13});

    assertEquals(10, OldSchemaReader.acceptedAncestor(12, childParent, accepted));
  }

  /** No accepted ancestor anywhere up the chain → 0 (no parentID written, becomes a root). */
  @Test
  public void testAcceptedAncestorNoneFound() {
    Int2IntOpenHashMap childParent = new Int2IntOpenHashMap();
    childParent.defaultReturnValue(0);
    childParent.put(21, 22); // -> 22
    childParent.put(22, 0);  // -> root
    IntOpenHashSet accepted = new IntOpenHashSet(); // nothing accepted

    assertEquals(0, OldSchemaReader.acceptedAncestor(21, childParent, accepted));
  }

  // accepted name_code → emitted taxon id
  private static final Map<String, String> ACC = Map.of(
      "A-acc", "t100",   // an accepted species
      "B-acc", "t200");  // another accepted species
  // synonym name_code → its declared accepted_name_code (may itself be a synonym = a chain)
  private static final Map<String, String> SYN = new HashMap<>() {{
    put("S1", "A-acc");  // direct: synonym of an accepted name
    put("S2", "S1");     // chain: synonym of a synonym (→ S1 → A-acc)
    put("S3", "S2");     // longer chain (→ S2 → S1 → A-acc)
    put("X1", "X-none"); // dead end: accepted_name_code never resolves
    put("C1", "C2");     // cycle
    put("C2", "C1");
  }};

  @Test
  public void testResolveAcceptedTaxon() {
    // an accepted name_code maps straight to its emitted taxon
    assertEquals("t100", OldSchemaReader.resolveAcceptedTaxon("A-acc", ACC, SYN));
    // a synonym resolves to its accepted taxon
    assertEquals("t100", OldSchemaReader.resolveAcceptedTaxon("S1", ACC, SYN));
    // synonym→synonym chains are followed to the accepted terminus
    assertEquals("t100", OldSchemaReader.resolveAcceptedTaxon("S2", ACC, SYN));
    assertEquals("t100", OldSchemaReader.resolveAcceptedTaxon("S3", ACC, SYN));
  }

  @Test
  public void testResolveAcceptedTaxonUnresolvable() {
    // accepted_name_code points to an unknown code → null (reference dropped, not dangling)
    assertNull(OldSchemaReader.resolveAcceptedTaxon("X1", ACC, SYN));
    // a name_code that is neither accepted nor a known synonym → null
    assertNull(OldSchemaReader.resolveAcceptedTaxon("ghost", ACC, SYN));
    assertNull(OldSchemaReader.resolveAcceptedTaxon(null, ACC, SYN));
    // a cycle terminates via the guard and yields null
    assertNull(OldSchemaReader.resolveAcceptedTaxon("C1", ACC, SYN));
  }

  @Test
  public void testRepairParent() {
    // an accepted homonym of the same binomial wins, regardless of the synonym's accepted_name_code
    // (e.g. a variety of "Monarda fistulosa" attaches to the accepted L. node, not the Sims synonym)
    assertEquals("t500", OldSchemaReader.repairParent(500, "S1", ACC, SYN));
    // no homonym → fall back to the synonym's accepted species via the accepted_name_code chain
    // (e.g. a variety of synonym "Stipa nelsonii" attaches to accepted "Achnatherum nelsonii")
    assertEquals("t100", OldSchemaReader.repairParent(null, "S2", ACC, SYN));
    // no homonym and the parent code does not resolve → null (caller walks up to the genus)
    assertNull(OldSchemaReader.repairParent(null, "X1", ACC, SYN));
    assertNull(OldSchemaReader.repairParent(null, null, ACC, SYN));
  }

  @Test
  public void testNameParentKey() {
    // distinct parents of a same-name homonym key apart; equal (name,parent) collide as intended
    assertEquals(OldSchemaReader.nameParentKey("Monarda fistulosa", 42),
        OldSchemaReader.nameParentKey("Monarda fistulosa", 42));
    assertNotEquals(OldSchemaReader.nameParentKey("Monarda fistulosa", 42),
        OldSchemaReader.nameParentKey("Monarda fistulosa", 7));
    // the separator prevents ("a1", 2) and ("a", 12) from colliding into "a12"
    assertNotEquals(OldSchemaReader.nameParentKey("a1", 2),
        OldSchemaReader.nameParentKey("a", 12));
  }
}
