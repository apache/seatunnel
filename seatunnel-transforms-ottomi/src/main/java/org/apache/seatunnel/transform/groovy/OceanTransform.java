package org.apache.seatunnel.transform.groovy;

public interface OceanTransform {

    Object[] transformRow(Object[] fields);
}
