package com.gamma.skybase.build.server.etl.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DMSSaleRecord {
    private String recordId;
    private String startSerial, endSerial;
    private String name, equipName;
    private Date submissionDate;
    private int yearId;
    private String monthName;
}
