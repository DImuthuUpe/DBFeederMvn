/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.sinmin.corpus.oracle.bean;

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
