package org.catalogueoflife.data.pbdb;

import java.util.Objects;

public class Person {
    private String oid;
    private String nam;
    private String ist;
    private String ctr;
    private String orc;
    private String rol;
    private String sta;

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getNam() {
        return nam;
    }

    public void setNam(String nam) {
        this.nam = nam;
    }

    public String getIst() {
        return ist;
    }

    public void setIst(String ist) {
        this.ist = ist;
    }

    public String getCtr() {
        return ctr;
    }

    public void setCtr(String ctr) {
        this.ctr = ctr;
    }

    public String getOrc() {
        return orc;
    }

    public void setOrc(String orc) {
        this.orc = orc;
    }

    public String getRol() {
        return rol;
    }

    public void setRol(String rol) {
        this.rol = rol;
    }

    public String getSta() {
        return sta;
    }

    public void setSta(String sta) {
        this.sta = sta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Person person)) return false;
        return Objects.equals(oid, person.oid) && Objects.equals(nam, person.nam) && Objects.equals(ist, person.ist) && Objects.equals(ctr, person.ctr) && Objects.equals(orc, person.orc) && Objects.equals(rol, person.rol) && Objects.equals(sta, person.sta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, nam, ist, ctr, orc, rol, sta);
    }
}
