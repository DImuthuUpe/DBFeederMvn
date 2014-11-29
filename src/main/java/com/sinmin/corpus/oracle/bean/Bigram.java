package com.sinmin.corpus.oracle.bean;

/**
 * Created by dimuthuupeksha on 11/2/14.
 */
public class Bigram {
    public Long id1,id2;
    public Bigram(long id1,long id2){
        this.id1= id1;
        this.id2 =id2;
    }

    @Override
    public int hashCode() {
        return 31*id1.hashCode()+id2.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Bigram)) {
            return false;
        }

        Bigram bigram = (Bigram) obj;
        return this.id1.longValue() == bigram.id1.longValue() && this.id2.longValue() == bigram.id2.longValue();
    }
}
