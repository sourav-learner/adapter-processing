package com.gamma.skybase.build.server.mongo;

public enum CollectionNames {

    SKYBASE_VOUCHER_CDR_CONTAINER ("skybase_voucher_cdr_container"),

    SDP_MAIN_BALANCE_LOOKUP("sdp_main_balance_lookup")
    ;

    /** The name. */
    private String name;

    /**
     * Instantiates a new table names.
     *
     * @param name the name
     */
    CollectionNames(String name){
        this.name = name;
    }

    /* (non-Javadoc)
     * @see java.lang.Enum#toString()
     */
    public String toString(){
        return name;
    }
}
