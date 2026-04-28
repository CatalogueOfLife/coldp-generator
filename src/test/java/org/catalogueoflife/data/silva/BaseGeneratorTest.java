package org.catalogueoflife.data.silva;

import org.junit.Test;
import static org.junit.Assert.*;

public class BaseGeneratorTest {

  // ── splitLine ─────────────────────────────────────────────────────────────

  @Test
  public void splitLineTypical() {
    String[] f = BaseGenerator.splitLine("Archaea;Crenarchaeota;\t12\tphylum\t\t119");
    assertNotNull(f);
    assertEquals(5, f.length);
    assertEquals("Archaea;Crenarchaeota;", f[0]);
    assertEquals("12",    f[1]);
    assertEquals("phylum", f[2]);
    assertEquals("",       f[3]);
    assertEquals("119",    f[4]);
  }

  @Test
  public void splitLineRootDomain() {
    // Root entry: no release version, empty remark
    String[] f = BaseGenerator.splitLine("Archaea;\t2\tdomain\t\t");
    assertNotNull(f);
    assertEquals("Archaea;", f[0]);
    assertEquals("2",        f[1]);
    assertEquals("domain",   f[2]);
    assertEquals("",         f[3]);
    assertEquals("",         f[4]);
  }

  @Test
  public void splitLineRemarkPresent() {
    String[] f = BaseGenerator.splitLine("Bacteria;uncultured;\t999\tphylum\ta\t138");
    assertEquals("a", f[3]);
  }

  @Test
  public void splitLineNullOrBlank() {
    assertNull(BaseGenerator.splitLine(null));
    assertNull(BaseGenerator.splitLine(""));
    assertNull(BaseGenerator.splitLine("   "));
  }

  // ── extractName ───────────────────────────────────────────────────────────

  @Test
  public void extractNameDeepPath() {
    assertEquals("Nitrososphaeria",
        BaseGenerator.extractName("Archaea;Crenarchaeota;Nitrososphaeria;"));
  }

  @Test
  public void extractNameRootPath() {
    assertEquals("Archaea", BaseGenerator.extractName("Archaea;"));
  }

  @Test
  public void extractNameCandidatus() {
    assertEquals("Candidatus Nitrosocaldus",
        BaseGenerator.extractName("Archaea;Crenarchaeota;Nitrososphaeria;Nitrosocaldales;Nitrosocaldaceae;Candidatus Nitrosocaldus;"));
  }

  // ── parentPath ────────────────────────────────────────────────────────────

  @Test
  public void parentPathDeep() {
    assertEquals("Archaea;Crenarchaeota;",
        BaseGenerator.parentPath("Archaea;Crenarchaeota;Nitrososphaeria;"));
  }

  @Test
  public void parentPathOneLevel() {
    assertEquals("Archaea;",
        BaseGenerator.parentPath("Archaea;Crenarchaeota;"));
  }

  @Test
  public void parentPathRoot() {
    // Single-segment paths have no parent
    assertNull(BaseGenerator.parentPath("Archaea;"));
  }

  // ── mapRemark ─────────────────────────────────────────────────────────────

  @Test
  public void mapRemarkA() {
    assertEquals("environmental origin", BaseGenerator.mapRemark("a"));
  }

  @Test
  public void mapRemarkW() {
    assertEquals("scheduled for revision", BaseGenerator.mapRemark("w"));
  }

  @Test
  public void mapRemarkEmpty() {
    assertNull(BaseGenerator.mapRemark(""));
    assertNull(BaseGenerator.mapRemark(null));
  }

  @Test
  public void mapRemarkUnknown() {
    assertNull(BaseGenerator.mapRemark("z"));
  }
}
