package com.gamma.skybase.build.utility;

import lombok.Data;

@Data
public class MTrunkGroup {
    private String id;
    private String type, direction, status;
    private String trunkprefix1, trunkprefix2, prefix;
    private String partner;
}
