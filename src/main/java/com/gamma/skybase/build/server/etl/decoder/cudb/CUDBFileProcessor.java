package com.gamma.skybase.build.server.etl.decoder.cudb;

import com.gamma.components.structure.IDatum;
import com.gamma.skybase.common.config.datasource.FileDataSource;
import com.gamma.skybase.contract.decoders.AFileSourceDecoder;
import com.gamma.skybase.contract.decoders.FileMetadata;
import com.gamma.skybase.contract.decoders.MRecordProcessorReq;
import com.gamma.skybase.contract.decoders.SkybaseDeserializerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class CUDBFileProcessor extends AFileSourceDecoder {

    private static final Logger logger = LoggerFactory.getLogger(CUDBFileProcessor.class);
    protected FileDataSource dataSource;
    protected FileMetadata metadata;

    @Override
    public void process(FileMetadata metadata) throws SkybaseDeserializerException {
        this.dataSource = (FileDataSource) super.dataSource;
        this.metadata = metadata;
        try {
//            System.out.println("\n\n\n\n "+this.metadata.toCSV());
            parseFile(this.metadata.decompFileURL);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new SkybaseDeserializerException("Issue Parsing File -> " + metadata.srcFileName, e);
        }
    }

//    public void handleEvents(String eventName, LinkedHashMap<String, Object> event) throws Exception {
//        try {
//            Map<String, IDatum> data;
//            MRecordProcessorReq req = new MRecordProcessorReq();
//            req.setEventName(eventName);
//            req.setEvent(event);
//            data = processor.handleEvents(req);
////            System.out.println("\n....................................\n");
//            if(data != null) {
////                System.out.println("\n-------------------------------------\n");
//                System.out.println(data.keySet().stream().collect(Collectors.joining(", ")));
//                System.out.println(data.values().stream()
//                        .map(Object::toString)
//                        .collect(Collectors.joining(", ")));
//                doCustomization(eventName, data);
////                System.out.println("\n\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\n");
//            }
//        }catch (Exception e){
////            e.printStackTrace();
//        }
//    }

    public void flush() throws SkybaseDeserializerException {
        super.flush();
    }
}