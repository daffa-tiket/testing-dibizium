package com.tiket.tix.hotel.common.debezium;

import static com.tiket.tix.hotel.common.component.K8sDataCollector.UNKNOWN;
import static com.tiket.tix.hotel.common.helper.CommonHelper.coalesceBlank;

import com.tiket.tix.common.monitor.aspects.Monitor.ServiceGroup;
import com.tiket.tix.common.monitor.enums.CustomTag;
import com.tiket.tix.common.monitor.enums.ErrorCode;
import com.tiket.tix.hotel.common.component.K8sDataCollector;
import com.tiket.tix.hotel.common.component.StaticBean;
import com.tiket.tix.hotel.common.model.constant.MetricEntity;
import java.util.HashMap;
import java.util.Map;

public class DebeziumHelper {
  public static void sendMetric(String type) {
    Map<CustomTag, String> tags = new HashMap<>();
    tags.put(CustomTag.TYPE, type);
    String hostName = coalesceBlank(
        K8sDataCollector.getHostname(), UNKNOWN);
    tags.put(CustomTag.ORIGIN, hostName);

    StaticBean.statsDClientWrapper.monitorCount("debezium-cdc-event", ServiceGroup.EVENTS, ErrorCode.SUCCEED,
        tags);
  }
}
