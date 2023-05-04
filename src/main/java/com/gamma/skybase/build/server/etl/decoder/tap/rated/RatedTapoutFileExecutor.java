package com.gamma.skybase.build.server.etl.decoder.tap.rated;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.FileUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.components.csv.MapToCsvUTF8;
import com.gamma.components.exceptions.AppUnexpectedException;
import com.gamma.components.structure.IDatum;
import com.gamma.decoder.ascii.DelimitedFileDecoder;
import com.gamma.skybase.build.server.service.SkybaseCacheServiceHandler;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.skybase.contract.decoders.SkybaseDeserializerException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.gamma.components.commons.constants.GammaConstants.PATH_SEP;

public class RatedTapoutFileExecutor extends RatedTapoutFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RatedTapoutFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;

    private boolean applyDuplicateCdrCheck = true;
    private List<Map<String, Object>> duplicates;
    private static Map<String, Object> cdrKeyLockMap = new ConcurrentHashMap<>();

    @Override
    public void parseFile(String fileName) throws Exception {
        boolean jsonOutputRequired = dataSource.isRawJsonEnabled();
        String fn = null;
        Gson gson = null;
        if (jsonOutputRequired) {
            fn = new File(fileName).getName();
            gson = new GsonBuilder().setPrettyPrinting().create();
            jsonRecords = new LinkedList<>();
        }

        IEnrichment enrichment = null;
        try {
            Class<?> exec = Class.forName(dataSource.getTxExecClass());
            enrichment = (IEnrichment) exec.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }

        String [] headers = {"event_date","start_time_charge_timestamp","start_time_offset","s_p_number_address","s_p_equipment_number","s_p_location_address","o_p_normed_num_address","call_type","tariff_info_sncode","duration_volume","rated_volume","rounded_volume","zero_rounded_volume_volume","eff_chrg_inf_xcurr_disc_amount","eff_charge_info_xcurr_tax","eff_chrg_inf_hcurr_disc_amount","record_id_tap_record_id","record_id_tap_sequence_num","record_id_udr_file_id","xfile_ind"};
        duplicates = new ArrayList<>();
        try (DelimitedFileDecoder decoder = new DelimitedFileDecoder(fileName, ',', true, true)) {
            logger.info("Adapter {} decoding File {} ", dataSource.getAdapterName(), new File(fileName).getName());
            long recCount = 0;
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    record.put("fileName", metadata.decompFileName);
                    String cdrKey = SkybaseCacheServiceHandler.instance().generateRatedTapoutCdrKey(record, dataSource.getAdapterName().toLowerCase());
                    recCount++;
                    if (!checkCdrDuplicateOrAdd(cdrKey, record)) {
                        continue;
                    }
                    if (jsonOutputRequired) {
                        jsonRecords.add(record);
                    }
                    processRecord(record, enrichment);
                    metadata.noOfParsedRecord++;
                } catch (Exception e) {
                    metadata.comments = "Parsing issues";
                    metadata.noOfBadRecord++;
                    logger.error(dataSource.getAdapterName() + " -> " + e.getMessage(), e);
                }
                recCount++;
                metadata.totalNoOfRecords = recCount;
            }
            if (metadata.noOfBadRecord > 0) metadata.status = "partial";
            if (metadata.noOfParsedRecord == 0 && recCount > 0) metadata.status = "failed";
        }
        if (jsonOutputRequired) {
            FileWriter writer = new FileWriter("out" + PATH_SEP + dataSource.getAdapterName() + PATH_SEP + fn + ".json");
            writer.write(gson.toJson(jsonRecords));
            writer.flush();
        }
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) throws Exception {
        if (enrichment != null) {
            MEnrichmentReq request = new MEnrichmentReq();
            request.setRequest(record);
            MEnrichmentResponse response = enrichment.transform(request);
            if (response.isResponseCode()) {
                LinkedHashMap<String, Object> data = response.getResponse();
                handleEvents("RATED_TAPOUT_FCT", data);
            }else {
                throw new AppUnexpectedException("Record enrichment failed.");
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }

    private boolean checkCdrDuplicateOrAdd(String cdrKey, LinkedHashMap<String, Object> record) {
        try {
            final Object sync = getSyncObjForCdrKey(cdrKey);
            synchronized (sync) {
                if (isDuplicateRecord(cdrKey)) {
                    duplicates.add(record);
                    return false;
                }
                addCdrKey(cdrKey, record);
                return true;
            }
        } finally {
            removeSyncObjForCdrKey(cdrKey);
        }
    }

    private void removeSyncObjForCdrKey(String cdrKey) {
        cdrKeyLockMap.remove(cdrKey);
    }

    private Object getSyncObjForCdrKey(String cdrKey) {
        return cdrKeyLockMap.computeIfAbsent(cdrKey, k -> new Object());
    }

    private boolean isDuplicateRecord(String cdrKey) {
        if (applyDuplicateCdrCheck) {
            if (SkybaseCacheServiceHandler.instance().checkIfDuplicateCDR(cdrKey)) {
                if (logger.isDebugEnabled()) logger.debug("[Rated-TAPOUT] Duplicate cdr found");
                return true;
            }
        }
        return false;
    }

    public void addCdrKey(String cdrKey, Map<String, Object> data) {
        if (applyDuplicateCdrCheck) {
            SkybaseCacheServiceHandler.instance().addRatedTapoutRecordsInCdrContainer(dataSource.getAdapterName().toLowerCase(), Arrays.asList(data));
        }
    }

    @Override
    @SuppressWarnings("Duplicates")
    public void flush() throws SkybaseDeserializerException {
        /* parent flush work */
        super.flush();

        /* cache flush */
        SkybaseCacheServiceHandler.instance().writeRatedTapoutCDREntries(dataSource.getAdapterName().toLowerCase(), null, true);

        /* cdr duplicate cdr file generation */
        if (applyDuplicateCdrCheck) {
            /* write duplicate cdr file */
            try {
                if (!duplicates.isEmpty()) {
                    logger.info("Total duplicate cdr found. source - {}, count - {}", dataSource.getAdapterName(), duplicates.size());
                    String directory = new File (dataSource.getDirConfig().getError().getPath()).getParent() + File.separator
                            + "duplicate" + File.separator + DateUtility.getTodayDateInShortString();
                    if (!new File(directory).isAbsolute()){
                        directory = AppConfig.instance().getHome() + File.separator + directory;
                    }
                    FileUtility.createDirectoryIfNotExists(directory);
                    File file = new File(directory + File.separator + metadata.decompFileName);
                    MapToCsvUTF8 m2c = new MapToCsvUTF8(file, true, '"');
                    for (Map<String, Object> row : duplicates) {
                        Map<String, String> r = new LinkedHashMap<>();
                        for (Map.Entry<String, Object> entry : row.entrySet()) {
                            r.put(entry.getKey(), String.valueOf(entry.getValue()));
                        }
                        m2c.write(r);
                    }
                    m2c.done();
                    duplicates.clear();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
