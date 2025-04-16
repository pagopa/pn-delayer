package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDispatchedDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryHighPriorityDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HighPriorityBatchServiceTest {

    @InjectMocks
    private HighPriorityBatchServiceImpl highPriorityService;

    @Mock
    private PaperDeliveryDriverCapacitiesDispatchedDAO paperDeliveryDispatchedCapacityDAO;

    @Mock
    private PaperDeliveryDriverCapacitiesDAO paperDeliveryCapacityDAO;

    @Mock
    private PaperDeliveryHighPriorityDAO paperDeliveryHighPriorityDAO;

    @Mock
    private PaperDeliveryUtils paperDeliveryUtils;

    @Mock
    private PnDelayerConfigs pnDelayerConfigs;

    @Test
    void initHighPriorityBatch_handlesEmptyItems() {
        when(paperDeliveryHighPriorityDAO.getPaperDeliveryHighPriority(anyString(), anyString(), anyMap()))
                .thenReturn(Mono.just(Page.create(Collections.emptyList())));

        highPriorityService.initHighPriorityBatch("1##RM", new HashMap<>()).block();

        verify(paperDeliveryHighPriorityDAO, times(1)).getPaperDeliveryHighPriority(anyString(), anyString(), anyMap());
        verify(paperDeliveryHighPriorityDAO, times(0)).executeTransaction(anyList(), anyList());
    }

    @Test
    void initHighPriorityBatch_processesMultipleChunks() {
        when(paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(anyString(), anyString(), any(), any()))
                .thenReturn(Mono.just(1));
        when(paperDeliveryDispatchedCapacityDAO.get(anyString(), anyString(), any()))
                .thenReturn(Mono.just(0));
        when(paperDeliveryDispatchedCapacityDAO.updateCounter(anyString(), anyString(), anyInt(), any()))
                .thenReturn(Mono.just(UpdateItemResponse.builder().build()));
        when(paperDeliveryUtils.calculateNextWeek(any())).thenReturn(Instant.now().plus(Duration.ofDays(7)));
        when(paperDeliveryUtils.filterAndPrepareDeliveries(anyList(), any(), any()))
                .thenReturn(1);
        when(paperDeliveryUtils.checkListsSize(any())).thenReturn(true);

        Page<PaperDeliveryHighPriority> page1 = getPaperDeliveryHighPriorityPage();
        page1.lastEvaluatedKey().put("lastEvaluatedKey", AttributeValue.builder().s("lastEvaluatedKey").build());
        Page<PaperDeliveryHighPriority> page2 = getPaperDeliveryHighPriorityPage();
        when(paperDeliveryHighPriorityDAO.getPaperDeliveryHighPriority(anyString(), anyString(), anyMap()))
                .thenReturn(Mono.just(page1))
                .thenReturn(Mono.just(page2));
        when(paperDeliveryUtils.checkCapacityAndFilterList(any(), anyList()))
                .thenReturn(page1.items())
                .thenReturn(page2.items());
        when(paperDeliveryUtils.groupDeliveryOnCapAndOrderOnCreatedAt(anyList()))
                .thenReturn(Map.of("00100", page1.items()))
                .thenReturn(Map.of("00100", page2.items()));


        when(paperDeliveryHighPriorityDAO.executeTransaction(anyList(), anyList())).thenReturn(Mono.empty());


        when(paperDeliveryHighPriorityDAO.executeTransaction(anyList(), anyList())).thenReturn(Mono.empty());

        highPriorityService.initHighPriorityBatch("1##RM", new HashMap<>()).block();

        verify(paperDeliveryHighPriorityDAO, times(2)).getPaperDeliveryHighPriority(anyString(), anyString(), anyMap());
        verify(paperDeliveryHighPriorityDAO, times(2)).executeTransaction(anyList(), anyList());
    }

    @Test
    void initHighPriorityBatch_handlesNullLastEvaluatedKey() {
        Page<PaperDeliveryHighPriority> page = getPaperDeliveryHighPriorityPage();
        when(paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(anyString(), anyString(), any(), any()))
                .thenReturn(Mono.just(1));
        when(paperDeliveryDispatchedCapacityDAO.get(anyString(), anyString(), any()))
                .thenReturn(Mono.just(0));
        when(paperDeliveryDispatchedCapacityDAO.updateCounter(anyString(), anyString(), anyInt(), any()))
                .thenReturn(Mono.just(UpdateItemResponse.builder().build()));
        when(paperDeliveryUtils.calculateNextWeek(any())).thenReturn(Instant.now().plus(Duration.ofDays(7)));
        when(paperDeliveryHighPriorityDAO.getPaperDeliveryHighPriority(anyString(), anyString(), anyMap()))
                .thenReturn(Mono.just(page));
        when(paperDeliveryUtils.calculateNextWeek(any())).thenReturn(Instant.now().plus(Duration.ofDays(7)));
        when(paperDeliveryUtils.filterAndPrepareDeliveries(anyList(), any(), any()))
                .thenReturn(1);
        when(paperDeliveryUtils.checkListsSize(any())).thenReturn(true);
        when(paperDeliveryHighPriorityDAO.getPaperDeliveryHighPriority(anyString(), anyString(), anyMap()))
                .thenReturn(Mono.just(page));
        when(paperDeliveryUtils.checkCapacityAndFilterList(any(), anyList()))
                .thenReturn(page.items());
        when(paperDeliveryUtils.groupDeliveryOnCapAndOrderOnCreatedAt(anyList()))
                .thenReturn(Map.of("00100", page.items()));
        when(paperDeliveryHighPriorityDAO.executeTransaction(anyList(), anyList())).thenReturn(Mono.empty());

        highPriorityService.initHighPriorityBatch("1##RM", new HashMap<>()).block();

        verify(paperDeliveryHighPriorityDAO, times(1)).getPaperDeliveryHighPriority(anyString(), anyString(), anyMap());
        verify(paperDeliveryHighPriorityDAO, times(1)).executeTransaction(anyList(), anyList());
    }

    @Test
    void initHighPriorityBatch_handlesNullLastEvaluatedKey_NoCapCapacity() {
        Page<PaperDeliveryHighPriority> page = getPaperDeliveryHighPriorityPage();
        when(paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(anyString(), anyString(), any(), any()))
                .thenReturn(Mono.just(1));
        when(paperDeliveryDispatchedCapacityDAO.get(anyString(), anyString(), any()))
                .thenReturn(Mono.just(0))
                .thenReturn(Mono.just(1));
        when(paperDeliveryUtils.calculateNextWeek(any())).thenReturn(Instant.now().plus(Duration.ofDays(7)));
        when(paperDeliveryHighPriorityDAO.getPaperDeliveryHighPriority(anyString(), anyString(), anyMap()))
                .thenReturn(Mono.just(page));

        highPriorityService.initHighPriorityBatch("1##RM", new HashMap<>()).block();

        verify(paperDeliveryHighPriorityDAO, times(1)).getPaperDeliveryHighPriority(anyString(), anyString(), anyMap());
        verify(paperDeliveryHighPriorityDAO, times(0)).executeTransaction(anyList(), anyList());
    }

    @Test
    void initHighPriorityBatch_handlesNullLastEvaluatedKey_NoProvinceCapacity() {
        Page<PaperDeliveryHighPriority> page = getPaperDeliveryHighPriorityPage();
        when(paperDeliveryCapacityDAO.getPaperDeliveryDriverCapacities(anyString(), anyString(), any(), any()))
                .thenReturn(Mono.just(1));
        when(paperDeliveryDispatchedCapacityDAO.get(anyString(), anyString(), any()))
                .thenReturn(Mono.just(1));
        when(paperDeliveryHighPriorityDAO.getPaperDeliveryHighPriority(anyString(), anyString(), anyMap()))
                .thenReturn(Mono.just(page));

        highPriorityService.initHighPriorityBatch("1##RM", new HashMap<>()).block();

        verify(paperDeliveryHighPriorityDAO, times(1)).getPaperDeliveryHighPriority(anyString(), anyString(), anyMap());
        verify(paperDeliveryHighPriorityDAO, times(0)).executeTransaction(anyList(), anyList());
    }

    private static Page<PaperDeliveryHighPriority> getPaperDeliveryHighPriorityPage() {
        PaperDeliveryHighPriority paperDeliveryHighPriority = new PaperDeliveryHighPriority();
        paperDeliveryHighPriority.setDeliveryDriverId("1");
        paperDeliveryHighPriority.setProvince("RM");
        paperDeliveryHighPriority.setCap("00100");
        paperDeliveryHighPriority.setTenderId("tenderId");
        paperDeliveryHighPriority.setDeliveryDriverIdGeoKey("1##RM");
        paperDeliveryHighPriority.setCreatedAt(Instant.now());

        return Page.create(
                List.of(paperDeliveryHighPriority),
                new HashMap<>()
        );
    }

}
