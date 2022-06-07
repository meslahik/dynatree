/*
 * Nest - A library for developing DSSMR-based services
 * Copyright (C) 2015, University of Lugano
 *
 *  This file is part of Nest.
 *
 *  Nest is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

package ch.usi.dslab.lel.dynastarv2.sample;


import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;

public class AppObject extends PRObject {
    private static final long serialVersionUID = 1l;

    int value;

    public AppObject() {
    }

    public AppObject(ObjId id, int initialValue) {
        super(id);
        value = initialValue;
    }

    @Override
    public void updateFromDiff(Message objectDiff) {
        objectDiff.rewind();
        this.value = (int) objectDiff.getNext();
    }

    @Override
    public Message getSuperDiff() {
        return new Message(this.value);
    }

    int getValue() {
        return value;
    }

    void add(int value) {
        this.value += value;
    }

    void sub(int value) {
        this.value -= value;
    }

    void mul(int value) {
        this.value *= value;
    }

    void div(int value) {
        this.value /= value;
    }

    @Override
    public String toString() {
        return "(" + this.getId() + ")=" + this.value;
    }
}
