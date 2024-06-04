package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BinaryType;
import org.apache.seatunnel.api.table.type.BinaryObject;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.commons.lang3.StringUtils;

import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;

/** @describe @Author jxm */
@Slf4j
public class BinaryReadStrategy extends AbstractReadStrategy {
    private CompressFormat compressFormat = BaseSourceConfigOptions.COMPRESS_CODEC.defaultValue();

    @Override
    public void init(HadoopConf conf) {
        super.init(conf);
        if (pluginConfig.hasPath(BaseSourceConfigOptions.COMPRESS_CODEC.key())) {
            String compressCodec =
                    pluginConfig.getString(BaseSourceConfigOptions.COMPRESS_CODEC.key());
            compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        InputStream inputStream;
        // 压缩格式应该无用
        switch (compressFormat) {
            case LZO:
                LzopCodec lzo = new LzopCodec();
                inputStream = lzo.createInputStream(hadoopFileSystemProxy.getInputStream(path));
                break;
            case NONE:
                inputStream = hadoopFileSystemProxy.getInputStream(path);
                break;
            default:
                log.warn(
                        "Binary file does not support this compress type: {}",
                        compressFormat.getCompressCodec());
                inputStream = hadoopFileSystemProxy.getInputStream(path);
                break;
        }
        try {
            int isFirst = 1;
            byte[] bys = new byte[1024 * 1024];
            int len = 0;
            int offset = 0;
            while (true) {
                len = inputStream.read(bys, offset, bys.length - offset);
                offset += len;
                if (offset >= bys.length) {
                    putData(tableId, output, path, bys, offset, isFirst);
                    isFirst = 0;
                    len = 0;
                    offset = 0;
                }
                if (len == -1) {
                    if (offset > 0) {
                        putData(tableId, output, path, bys, offset, isFirst);
                    }
                    break;
                }
            }
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("BinaryFile", "read", path, e);
        }
    }

    private void putData(
            String tableId, Collector<SeaTunnelRow> output, String path, byte[] bys, int len, int isFirst) {
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(1);
        seaTunnelRow.setField(0,
                new BinaryObject(
                        StringUtils.substringAfterLast(path, File.separator),
                        // TODO 使用Base64解决乱码
                        Base64.getEncoder().encodeToString(bys),
                        len,
                        isFirst));
        seaTunnelRow.setTableId(tableId);
        output.collect(seaTunnelRow);
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        seaTunnelRowType = new SeaTunnelRowType(
                // TODO
                new String[]{"binary"},
                new SeaTunnelDataType[]{BinaryType.INSTANCE}
        );
        return seaTunnelRowType;
    }
}
