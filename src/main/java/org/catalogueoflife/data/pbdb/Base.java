package org.catalogueoflife.data.pbdb;

import java.util.Objects;

public class Base {
    private String oid; // ID
    private String ati; // The identifier of the person who authorized the entry of this record
    private String ath; // The name of the person who authorized the entry of this record
    private String eni; // The identifier of the person who actually entered this record.
    private String ent; // The name of the person who actually entered this record
    private String mdi; // The identifier of the person who last modified this record, if it has been modified.
    private String mdf; // The name of the person who last modified this record, if it has been modified.
    private String dcr; // The date and time at which this record was created.
    private String dmd; // The date and time at which this record was last modified.
    private String rid; // The identifier of the reference from which this name was entered.

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Base base)) return false;
        return Objects.equals(oid, base.oid) && Objects.equals(ati, base.ati) && Objects.equals(ath, base.ath) && Objects.equals(eni, base.eni) && Objects.equals(ent, base.ent) && Objects.equals(mdi, base.mdi) && Objects.equals(mdf, base.mdf) && Objects.equals(dcr, base.dcr) && Objects.equals(dmd, base.dmd) && Objects.equals(rid, base.rid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, ati, ath, eni, ent, mdi, mdf, dcr, dmd, rid);
    }
}
