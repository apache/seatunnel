/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.resourcemanager.resource;

import java.io.Serializable;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

public class ResourceProfile implements Serializable {

    private final CPU cpu;

    private final Memory heapMemory;

    public ResourceProfile() {
        this.cpu = CPU.of(0);
        this.heapMemory = Memory.of(0);
    }

    public ResourceProfile(CPU cpu, Memory heapMemory) {
        checkArgument(cpu.getCore() >= 0, "The cpu core cannot be negative");
        checkArgument(heapMemory.getBytes() >= 0, "The heapMemory bytes cannot be negative");
        this.cpu = cpu;
        this.heapMemory = heapMemory;
    }

    public CPU getCpu() {
        return cpu;
    }

    public Memory getHeapMemory() {
        return heapMemory;
    }

    public ResourceProfile merge(ResourceProfile other) {
        CPU c = CPU.of(this.cpu.getCore() + other.getCpu().getCore());
        Memory m = Memory.of(this.heapMemory.getBytes() + other.heapMemory.getBytes());
        return new ResourceProfile(c, m);
    }

    public ResourceProfile subtract(ResourceProfile other) {
        CPU c = CPU.of(this.cpu.getCore() - other.getCpu().getCore());
        Memory m = Memory.of(this.heapMemory.getBytes() - other.heapMemory.getBytes());
        return new ResourceProfile(c, m);
    }

    public boolean enoughThan(ResourceProfile other) {
        return this.cpu.getCore() >= other.getCpu().getCore()
                && this.heapMemory.getBytes() >= other.getHeapMemory().getBytes();
    }

    @Override
    public String toString() {
        return "ResourceProfile{" + "cpu=" + cpu + ", heapMemory=" + heapMemory + '}';
    }
}
