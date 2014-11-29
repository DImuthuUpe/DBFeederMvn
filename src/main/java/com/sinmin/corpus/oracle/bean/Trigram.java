package com.sinmin.corpus.oracle.bean;

/**
 * Created by dimuthuupeksha on 11/2/14.
 */
public class Trigram {
    public Long id1,id2,id3;
    public Trigram(long id1,long id2,long id3){
        this.id1= id1;
        this.id2 =id2;
        this.id3 = id3;
    }

    @Override
    public int hashCode() {
        int t= 31*id1.hashCode()+id2.hashCode();
        return 31*t + id3.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Trigram)) {
            return false;
        }

        Trigram trigram = (Trigram) obj;
        return this.id1.longValue() == trigram.id1.longValue() && this.id2.longValue() == trigram.id2.longValue()&& this.id3.longValue() == trigram.id3.longValue();
    }
}
