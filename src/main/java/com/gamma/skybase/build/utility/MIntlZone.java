package com.gamma.skybase.build.utility;

import lombok.Data;

@Data
public class MIntlZone {
    private String dialCode;
    private String country, zone;
    private double ppm, pps;
    private String minPulse;
}
