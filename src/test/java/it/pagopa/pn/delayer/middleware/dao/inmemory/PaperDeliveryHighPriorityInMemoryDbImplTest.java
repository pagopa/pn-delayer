package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaperDeliveryHighPriorityInMemoryDbImplTest{

    PaperDeliveryHighPriorityInMemoryDbImpl dao;

    @Mock
    PaperDeliveryReadyToSendInMemoryDbImpl paperDeliveryReadyToSendInMemoryDb;

    @Mock
    PnDelayerConfigs pnDelayerConfigs;

    @BeforeEach
    void setUp() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        dao = new PaperDeliveryHighPriorityInMemoryDbImpl(pnDelayerConfigs, paperDeliveryReadyToSendInMemoryDb, objectMapper);
    }


    @Test
    void delete_handlesEmptyListToRemove() {
        List<PaperDeliveryHighPriority> paperDeliveryHighPriorities = dao.get("4~IM")
                        .stream().limit(1).toList();

        dao.delete("4~IM", paperDeliveryHighPriorities).block();
        Assertions.assertEquals(499, dao.get("4~IM").size());
    }

    @Test
    void get_returnsListForExistingKey() {
        List<PaperDeliveryHighPriority> result = dao.get("5~PA");
        Assertions.assertEquals(500, result.size());
        Assertions.assertTrue(result.stream().allMatch(item -> item.getUnifiedDeliveryDriverGeoKey().equals("5~PA")));
    }

    @Test
    void get_returnsEmptyListForNonExistingKey() {
        String pk = "nonExistingKey";
        List<PaperDeliveryHighPriority> result = dao.get(pk);
        Assertions.assertEquals(Collections.emptyList(), result);
    }

    @Test
    void getPaperDeliveryHighPriority_returnsLimitedResults() {
        when(pnDelayerConfigs.getHighPriorityQueryLimit()).thenReturn(5);
        Page<PaperDeliveryHighPriority> result = dao.getPaperDeliveryHighPriority("3", "GR", null).block();
        assert result != null;
        Assertions.assertEquals(5, result.items().size());
    }

    @Test
    void getPaperDeliveryHighPriority_returnsEmptyPageForNonExistingKey() {
        String unifiedDeliveryDriver = "driver2";
        String geoKey = "geo2";
        Page<PaperDeliveryHighPriority> result = dao.getPaperDeliveryHighPriority(unifiedDeliveryDriver, geoKey, null).block();
        assert result != null;
        Assertions.assertEquals(Collections.emptyList(), result.items());
    }

    @Test
    void executeTransaction_movesItemsToReadyToSendAndDeletesFromHighPriority() {
        String pk = "5~PA";
        List<PaperDeliveryHighPriority> highPriorities = dao.get(pk);
        List<PaperDeliveryReadyToSend> readyToSendList = List.of(
                new PaperDeliveryReadyToSend()
        );

        when(paperDeliveryReadyToSendInMemoryDb.insert(anyList())).thenReturn(Mono.just(1));

        dao.executeTransaction(highPriorities, readyToSendList).block();

        Assertions.assertEquals(Collections.emptyList(), dao.get(pk));
    }
}
