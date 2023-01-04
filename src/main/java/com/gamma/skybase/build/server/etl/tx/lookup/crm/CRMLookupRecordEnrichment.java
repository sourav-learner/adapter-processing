package com.gamma.skybase.build.server.etl.tx.lookup.crm;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;

@Slf4j
public class CRMLookupRecordEnrichment implements IEnrichment {

    @Override
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        LinkedHashMap<String, Object> data = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();
        if (data != null) {
            response.setResponseCode(true);
            response.setResponse(data);
        } else response.setResponseCode(false);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        return new LinkedHashMap<>(data); //Nothing to add
    }
}
