package org.catalogueoflife.data.pbdb;

import java.util.Objects;

/**
 * > {"oid":"txn:71894","vid":"var:285777","flg":"V","rnk":5,
 * "nam":"Dascyllus","tdf":"misspelling of","acc":"txn:71894",
 * "acr":5,"acn":"Dascillus","par":"txn:70061","rid":"ref:56433","ext":"1","noc":23,
 * "tei":"Priabonian","tli":"Chibanian","siz":9,"exs":2,
 * "phl":"Arthropoda","cll":"Insecta","odl":"Coleoptera",
 * "fml":"Dascillidae","gnl":"Dascillus","jev":"terrestrial",
 * "jec":"Insecta","jmo":"actively mobile","jco":"chitin"},
 *
 */
public class Taxon {
    private String oid; // ID
    private String vid; // A unique identifier for the selected variant of this taxonomic name. By default, this is the variant currently accepted as most correct.
    private String flg; // flags: This field will be empty for most records. Otherwise, it will contain one or more of the following letters: I= taxon is an ichnotaxon. F= taxon is a form taxon.
    private Integer rnk; // rank
    private String nam; // name
    private String att; // attribution: name authorship
    private String nm2; // The common (vernacular) name of this taxon, if any
    private String tdf; // If this name is either a junior synonym or is invalid for some reason, this field gives the reason. The fields accepted_no and accepted_name then specify the name that should be used instead.
    private String acc; // If this name is either a junior synonym or an invalid name, this field gives the identifier of the accepted name to be used in its place. Otherwise, its value will be the same as orig_no. In the compact vocabulary, this field will be omitted in that case.
    private String acn; // accepted name
    private String ext; // True if this taxon is extant on earth today, false if not, not present if unrecorded
    private String img; // If this value is non-zero, you can use it to construct image URLs using taxa/thumb and taxa/icon.

    private String ati; // The identifier of the person who authorized the entry of this record
    private String ath; // The name of the person who authorized the entry of this record
    private String eni; // The identifier of the person who actually entered this record.
    private String ent; // The name of the person who actually entered this record
    private String mdi; // The identifier of the person who last modified this record, if it has been modified.
    private String mdf; // The name of the person who last modified this record, if it has been modified.
    private String dcr; // The date and time at which this record was created.
    private String dmd; // The date and time at which this record was last modified.

    private String rid; // The identifier of the reference from which this name was entered.

    // geotimes
    private Float fea; // The early age bound for the first appearance of this taxon in the database
    private Float fla; // The late age bound for the first appearance of this taxon in the database
    private Float lea; // The early age bound for the last appearance of this taxon in the database
    private Float lla; // The late age bound for the last appearance of this taxon in the database

    private String tei; // The name of the interval in which this taxon first appears, or the start of its range.
    private String tli; // The name of the interval in which this taxon last appears, if different from early_interval.

    // counts
    private Integer siz; // The total number of taxa in the database that are contained within this taxon, including itself
    private Integer exs; // The total number of extant taxa in the database that are contained within this taxon, including itself
    private String noc; // The number of fossil occurrences in this database that are identified as belonging to this taxon or any of its subtaxa.

    // classification parents
    private String prt; // parent taxon
    private String prl; // The name of the parent taxon or its senior synonym if any
    private String prr; // The rank of the parent taxon or its senior synonym if any
    private String par; // The identifier of the parent taxon, or of its senior synonym if there is one. This field and those following are only available if the classification of this taxon is known to the database.
    private String ipn; // The identifier of the immediate parent taxon, even if it is a junior synonym.
    private String ipl; // The name of the immediate parent taxon, even if it is a junior synonym.

    // classification flat
    private String kgn; // kingdom ID
    private String kgl; // kingdom name
    private String phn; // The identifier of the phylum in which this taxon is classified. This is only included with the block classext.
    private String phl; // The name of the phylum in which this taxon is classified
    private String cln; // The identifier of the class in which this taxon is classified. This is only included with the block classext.
    private String cll; // The name of the class in which this taxon is classified
    private String odn; // The identifier of the order in which this occurrence is classified. This is only included with the block classext.
    private String odl; // The name of the order in which this taxon is classified
    private String fmn; // The identifier of the family in which this occurrence is classified. This is only included with the block classext.
    private String fml; // The name of the family in which this taxon is classified
    private String gnn; // The identifier of the genus in which this occurrence is classified. This is only included with the block classext.
    private String gnl; // The name of the genus in which this taxon is classified. A genus may be listed as occurring in a different genus if it is a junior synonym; a species may be listed as occurring in a different genus than its name would indicate if its genus is synonymized but no synonymy opinion has been entered for the species.

    // type
    private String ttl; // The name of the type taxon for this taxon, if known.
    private String ttn; // The identifier of the type taxon for this taxon, if known.

    // facts
    private String jec; // environment_basis: Insecta
    private String jev; // environment: terrestrial
    private String jmo; // actively mobile
    private String jlh; // habit
    private String jvs; // vision
    private String jdt; // diet
    private String jre; // reproduction
    private String jon; // ontogeny
    private String jco; // composition: chitin

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getFlg() {
        return flg;
    }

    public void setFlg(String flg) {
        this.flg = flg;
    }

    public Integer getRnk() {
        return rnk;
    }

    public void setRnk(Integer rnk) {
        this.rnk = rnk;
    }

    public String getNam() {
        return nam;
    }

    public void setNam(String nam) {
        this.nam = nam;
    }

    public String getNm2() {
        return nm2;
    }

    public void setNm2(String nm2) {
        this.nm2 = nm2;
    }

    public String getPar() {
        return par;
    }

    public void setPar(String par) {
        this.par = par;
    }

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getExt() {
        return ext;
    }

    public Boolean isExtant() {
        return ext == null ? null :
                ext.equalsIgnoreCase("1") || ext.equalsIgnoreCase("true");
    }

    public void setExt(String ext) {
        this.ext = ext;
    }

    public String getJlh() {
        return jlh;
    }

    public void setJlh(String jlh) {
        this.jlh = jlh;
    }

    public String getJvs() {
        return jvs;
    }

    public void setJvs(String jvs) {
        this.jvs = jvs;
    }

    public String getJdt() {
        return jdt;
    }

    public void setJdt(String jdt) {
        this.jdt = jdt;
    }

    public String getJre() {
        return jre;
    }

    public void setJre(String jre) {
        this.jre = jre;
    }

    public String getJon() {
        return jon;
    }

    public void setJon(String jon) {
        this.jon = jon;
    }

    public String getNoc() {
        return noc;
    }

    public void setNoc(String noc) {
        this.noc = noc;
    }

    public String getTei() {
        return tei;
    }

    public void setTei(String tei) {
        this.tei = tei;
    }

    public String getTli() {
        return tli;
    }

    public void setTli(String tli) {
        this.tli = tli;
    }

    public Integer getSiz() {
        return siz;
    }

    public void setSiz(Integer siz) {
        this.siz = siz;
    }

    public Integer getExs() {
        return exs;
    }

    public void setExs(Integer exs) {
        this.exs = exs;
    }

    public String getPhl() {
        return phl;
    }

    public void setPhl(String phl) {
        this.phl = phl;
    }

    public String getCll() {
        return cll;
    }

    public void setCll(String cll) {
        this.cll = cll;
    }

    public String getOdl() {
        return odl;
    }

    public void setOdl(String odl) {
        this.odl = odl;
    }

    public String getFml() {
        return fml;
    }

    public void setFml(String fml) {
        this.fml = fml;
    }

    public String getJev() {
        return jev;
    }

    public void setJev(String jev) {
        this.jev = jev;
    }

    public String getJec() {
        return jec;
    }

    public void setJec(String jec) {
        this.jec = jec;
    }

    public String getJmo() {
        return jmo;
    }

    public void setJmo(String jmo) {
        this.jmo = jmo;
    }

    public String getJco() {
        return jco;
    }

    public void setJco(String jco) {
        this.jco = jco;
    }

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public String getAtt() {
        return att;
    }

    public void setAtt(String att) {
        this.att = att;
    }

    public String getTdf() {
        return tdf;
    }

    public void setTdf(String tdf) {
        this.tdf = tdf;
    }

    public String getAcc() {
        return acc;
    }

    public void setAcc(String acc) {
        this.acc = acc;
    }

    public String getAcn() {
        return acn;
    }

    public void setAcn(String acn) {
        this.acn = acn;
    }

    public String getAti() {
        return ati;
    }

    public void setAti(String ati) {
        this.ati = ati;
    }

    public String getAth() {
        return ath;
    }

    public void setAth(String ath) {
        this.ath = ath;
    }

    public String getEni() {
        return eni;
    }

    public void setEni(String eni) {
        this.eni = eni;
    }

    public String getEnt() {
        return ent;
    }

    public void setEnt(String ent) {
        this.ent = ent;
    }

    public String getMdi() {
        return mdi;
    }

    public void setMdi(String mdi) {
        this.mdi = mdi;
    }

    public String getMdf() {
        return mdf;
    }

    public void setMdf(String mdf) {
        this.mdf = mdf;
    }

    public String getDcr() {
        return dcr;
    }

    public void setDcr(String dcr) {
        this.dcr = dcr;
    }

    public String getDmd() {
        return dmd;
    }

    public void setDmd(String dmd) {
        this.dmd = dmd;
    }

    public Float getFea() {
        return fea;
    }

    public void setFea(Float fea) {
        this.fea = fea;
    }

    public Float getFla() {
        return fla;
    }

    public void setFla(Float fla) {
        this.fla = fla;
    }

    public Float getLea() {
        return lea;
    }

    public void setLea(Float lea) {
        this.lea = lea;
    }

    public Float getLla() {
        return lla;
    }

    public void setLla(Float lla) {
        this.lla = lla;
    }

    public String getImg() {
        return img;
    }

    public void setImg(String img) {
        this.img = img;
    }

    public String getPrt() {
        return prt;
    }

    public void setPrt(String prt) {
        this.prt = prt;
    }

    public String getPrl() {
        return prl;
    }

    public void setPrl(String prl) {
        this.prl = prl;
    }

    public String getPrr() {
        return prr;
    }

    public void setPrr(String prr) {
        this.prr = prr;
    }

    public String getIpn() {
        return ipn;
    }

    public void setIpn(String ipn) {
        this.ipn = ipn;
    }

    public String getIpl() {
        return ipl;
    }

    public void setIpl(String ipl) {
        this.ipl = ipl;
    }

    public String getKgn() {
        return kgn;
    }

    public void setKgn(String kgn) {
        this.kgn = kgn;
    }

    public String getKgl() {
        return kgl;
    }

    public void setKgl(String kgl) {
        this.kgl = kgl;
    }

    public String getPhn() {
        return phn;
    }

    public void setPhn(String phn) {
        this.phn = phn;
    }

    public String getCln() {
        return cln;
    }

    public void setCln(String cln) {
        this.cln = cln;
    }

    public String getOdn() {
        return odn;
    }

    public void setOdn(String odn) {
        this.odn = odn;
    }

    public String getFmn() {
        return fmn;
    }

    public void setFmn(String fmn) {
        this.fmn = fmn;
    }

    public String getGnn() {
        return gnn;
    }

    public void setGnn(String gnn) {
        this.gnn = gnn;
    }

    public String getGnl() {
        return gnl;
    }

    public void setGnl(String gnl) {
        this.gnl = gnl;
    }

    public String getTtl() {
        return ttl;
    }

    public void setTtl(String ttl) {
        this.ttl = ttl;
    }

    public String getTtn() {
        return ttn;
    }

    public void setTtn(String ttn) {
        this.ttn = ttn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Taxon taxon)) return false;
        return Objects.equals(oid, taxon.oid) && Objects.equals(vid, taxon.vid) && Objects.equals(flg, taxon.flg) && Objects.equals(rnk, taxon.rnk) && Objects.equals(nam, taxon.nam) && Objects.equals(att, taxon.att) && Objects.equals(nm2, taxon.nm2) && Objects.equals(tdf, taxon.tdf) && Objects.equals(acc, taxon.acc) && Objects.equals(acn, taxon.acn) && Objects.equals(ati, taxon.ati) && Objects.equals(ath, taxon.ath) && Objects.equals(eni, taxon.eni) && Objects.equals(ent, taxon.ent) && Objects.equals(mdi, taxon.mdi) && Objects.equals(mdf, taxon.mdf) && Objects.equals(dcr, taxon.dcr) && Objects.equals(dmd, taxon.dmd) && Objects.equals(rid, taxon.rid) && Objects.equals(ext, taxon.ext) && Objects.equals(noc, taxon.noc) && Objects.equals(fea, taxon.fea) && Objects.equals(fla, taxon.fla) && Objects.equals(lea, taxon.lea) && Objects.equals(lla, taxon.lla) && Objects.equals(tei, taxon.tei) && Objects.equals(tli, taxon.tli) && Objects.equals(siz, taxon.siz) && Objects.equals(exs, taxon.exs) && Objects.equals(img, taxon.img) && Objects.equals(prt, taxon.prt) && Objects.equals(prl, taxon.prl) && Objects.equals(prr, taxon.prr) && Objects.equals(par, taxon.par) && Objects.equals(ipn, taxon.ipn) && Objects.equals(ipl, taxon.ipl) && Objects.equals(kgn, taxon.kgn) && Objects.equals(kgl, taxon.kgl) && Objects.equals(phn, taxon.phn) && Objects.equals(phl, taxon.phl) && Objects.equals(cln, taxon.cln) && Objects.equals(cll, taxon.cll) && Objects.equals(odn, taxon.odn) && Objects.equals(odl, taxon.odl) && Objects.equals(fmn, taxon.fmn) && Objects.equals(fml, taxon.fml) && Objects.equals(gnn, taxon.gnn) && Objects.equals(gnl, taxon.gnl) && Objects.equals(ttl, taxon.ttl) && Objects.equals(ttn, taxon.ttn) && Objects.equals(jec, taxon.jec) && Objects.equals(jev, taxon.jev) && Objects.equals(jmo, taxon.jmo) && Objects.equals(jlh, taxon.jlh) && Objects.equals(jvs, taxon.jvs) && Objects.equals(jdt, taxon.jdt) && Objects.equals(jre, taxon.jre) && Objects.equals(jon, taxon.jon) && Objects.equals(jco, taxon.jco);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, vid, flg, rnk, nam, att, nm2, tdf, acc, acn, ati, ath, eni, ent, mdi, mdf, dcr, dmd, rid, ext, noc, fea, fla, lea, lla, tei, tli, siz, exs, img, prt, prl, prr, par, ipn, ipl, kgn, kgl, phn, phl, cln, cll, odn, odl, fmn, fml, gnn, gnl, ttl, ttn, jec, jev, jmo, jlh, jvs, jdt, jre, jon, jco);
    }
}
