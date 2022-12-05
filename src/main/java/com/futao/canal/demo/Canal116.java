package com.futao.canal.demo;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.CollectionUtils;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * canal 1.1.6
 * 1.1.6，使用的1.1.4的客户端，1.1.6的客户端缺包
 *
 * @author futao
 * @since 2022/12/5
 */
@Slf4j
public class Canal116 {

    public static void main(String[] args) throws InvalidProtocolBufferException, InterruptedException {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("localhost", 11111), "canal_slave", StringUtils.EMPTY, StringUtils.EMPTY);
        canalConnector.connect();
        canalConnector.subscribe(".*\\..*");
        // canalConnector.rollback();
        while (true) {
            Message message = canalConnector.getWithoutAck(100);
            long messageId = message.getId();
            List<CanalEntry.Entry> entries = message.getEntries();
            if (CollectionUtils.isEmpty(entries)) {
                log.info("entries为空，休眠2秒");
                TimeUnit.SECONDS.sleep(2L);
            }
            log.info("entry size: {}", entries.size());
            log.info("-------------------------------------------------");
            for (CanalEntry.Entry entry : entries) {
                // 一个entry对应一条SQL语句
                CanalEntry.EntryType entryType = entry.getEntryType();
                String schemaName = entry.getHeader().getSchemaName();
                String tableName = entry.getHeader().getTableName();
                log.info("操作的schemaName={}, tableName={}", schemaName, tableName);
                log.info("entryType为：{}", entryType);
                if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                    // 操作类型为对行数据的操作
                    // 获取数据
                    ByteString storeValue = entry.getStoreValue();
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    log.info("event type: {}", eventType);
                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                    log.info("影响的数据行: {}", rowDatasList.size());
                    for (CanalEntry.RowData rowData : rowDatasList) {
                        List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                        JSONObject before = new JSONObject();
                        beforeColumnsList.forEach(x -> before.put(x.getName(), x.getValue()));
                        List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                        JSONObject after = new JSONObject();
                        afterColumnsList.forEach(x -> after.put(x.getName(), x.getValue()));
                        log.info("before:{}\r\nafter:{}", before, after);
                    }
                }
            }
            canalConnector.ack(messageId);
        }
    }
}
