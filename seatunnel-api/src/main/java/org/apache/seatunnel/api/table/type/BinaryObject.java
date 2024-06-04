package org.apache.seatunnel.api.table.type;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BinaryObject implements Serializable {

    private String name;

    private String bytes;

    private int len;

    private int isFirst;

}
