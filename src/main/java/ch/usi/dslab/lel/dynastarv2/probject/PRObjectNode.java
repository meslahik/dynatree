package ch.usi.dslab.lel.dynastarv2.probject;
/*
 * ScalableSMR - A library for developing Scalable services based on SMR
 * Copyright (C) 2017, University of Lugano
 *
 *  This file is part of ScalableSMR.
 *
 *  ScalableSMR is free software; you can redistribute it and/or
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

import ch.usi.dslab.bezerra.netwrapper.codecs.Codec;
import ch.usi.dslab.bezerra.netwrapper.codecs.CodecUncompressedKryo;

import java.io.Serializable;

/**
 * Created by longle on 02.08.17.
 */

public class PRObjectNode implements Comparable<PRObjectNode>, Serializable {
    private int partitionId;
    private int ownerPartitionId = -1;
    private ObjId objId;
    private PRObject prObject;
    private boolean isReserved = false;

    public PRObjectNode() {

    }

    public PRObjectNode(PRObject prObject, ObjId objId, int partitionId) {
        this.prObject = prObject;
        this.objId = objId;
        this.partitionId = partitionId;
    }

    public PRObjectNode(ObjId objId, int partitionId) {
        this(null, objId, partitionId);
    }

    public int getOwnerPartitionId() {
        return ownerPartitionId;
    }

    public void setOwnerPartitionId(int ownerPartitionId) {
        this.ownerPartitionId = ownerPartitionId;
    }

    public boolean isReserved() {
        return isReserved;
    }

    public void setReserved(boolean reserved) {
        isReserved = reserved;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public ObjId getId() {
        return objId;
    }

    public void setId(ObjId objId) {
        this.objId = objId;
    }

    @Override
    public int compareTo(PRObjectNode o) {
        return this.getId().compareTo(o.getId());
    }

    @Override
    public boolean equals(Object other_) {
        try {
            PRObjectNode other = ((PRObjectNode) other_);
            return this.getId().equals(other.getId());
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + this.getId().value;
        result = 31 * result + this.partitionId;
        return result;
//        int hashCode = this.getId().value;
//        hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
//        return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
    }

    public PRObject getPRObject() {
        return prObject;
    }

    public void setPRObject(PRObject prObject) {
        this.prObject = prObject;
    }

    public void update(int partitionId) {
        setPartitionId(partitionId);
    }

    @Override
    public String toString() {
        if (this.prObject != null) return "[OK/" + this.partitionId + "]" + this.prObject.toString();
        return "[NO/" + this.partitionId + "]" + this.objId.toString();
    }

    public PRObjectNode deepClone() {
        Codec codec = new CodecUncompressedKryo();
        return (PRObjectNode) codec.deepDuplicate(this);
    }
}
