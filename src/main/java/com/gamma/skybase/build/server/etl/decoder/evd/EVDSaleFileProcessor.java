package com.gamma.skybase.build.server.etl.decoder.evd;

import com.gamma.skybase.common.config.datasource.FileDataSource;
import com.gamma.skybase.contract.decoders.AFileSourceDecoder;
import com.gamma.skybase.contract.decoders.FileMetadata;
import com.gamma.skybase.contract.decoders.SkybaseDeserializerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EVDSaleFileProcessor extends AFileSourceDecoder {

    private static final Logger logger = LoggerFactory.getLogger(EVDSaleFileProcessor.class);
    protected FileDataSource dataSource;
    protected FileMetadata metadata;

    @Override
    public void process(FileMetadata metadata) throws SkybaseDeserializerException {
        this.dataSource = (FileDataSource) super.dataSource;
        this.metadata = metadata;
        try {
            parseFile(this.metadata.decompFileURL);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new SkybaseDeserializerException("Issue Parsing File -> " + metadata.srcFileName, e);
        }
    }

    public void flush() throws SkybaseDeserializerException{
        super.flush();
    }

}
