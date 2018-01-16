package com.bigdata.model;

import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Iterator;

import static com.google.common.base.Splitter.on;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;

@Table(keyspace = "uservisits_scheme", name = "uservisit")
public class Uservisit implements Serializable {
    private String sourceip;
    private String desturl;
    private String visitdate;
    private Float adrevenue;
    private String useragent;
    private String countrycode;
    private String languagecode;
    private String searchword;
    private Integer duration;

    public Uservisit(String sourceip, String desturl, String visitdate,
                     Float adrevenue, String useragent, String countrycode,
                     String languagecode, String searchword, Integer duration) {
        this.sourceip = sourceip;
        this.desturl = desturl;
        this.visitdate = visitdate;
        this.adrevenue = adrevenue;
        this.useragent = useragent;
        this.countrycode = countrycode;
        this.languagecode = languagecode;
        this.searchword = searchword;
        this.duration = duration;
    }

    public static Uservisit buildUservisitModelFromString(String record) {
        Iterator<String> values = on(",").split(record).iterator();

        return new Uservisit(
                values.next(),
                values.next(),
                values.next(),
                parseFloat(values.next()),
                values.next(),
                values.next(),
                values.next(),
                values.next(),
                parseInt(values.next())
        );
    }
}
