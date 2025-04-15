package it.pagopa.pn.delayer.middleware.dao.inmemory;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class PaperDeliveryReadyToSendInMemoryDbImplTest {

    @InjectMocks
    PaperDeliveryReadyToSendInMemoryDbImpl dao;

    @Test
    void insert_addsAllItemsToDataStore() {
        PaperDeliveryReadyToSend paperDeliveryReadyToSend = new PaperDeliveryReadyToSend();
        paperDeliveryReadyToSend.setDeliveryDate(Instant.parse("2023-10-01T10:00:00Z"));
        paperDeliveryReadyToSend.setRequestId("req1");

        PaperDeliveryReadyToSend paperDeliveryReadyToSend2 = new PaperDeliveryReadyToSend();
        paperDeliveryReadyToSend2.setDeliveryDate(Instant.parse("2023-10-02T10:00:00Z"));
        paperDeliveryReadyToSend2.setRequestId("req2");

        PaperDeliveryReadyToSend paperDeliveryReadyToSend3 = new PaperDeliveryReadyToSend();
        paperDeliveryReadyToSend3.setDeliveryDate(Instant.parse("2023-10-02T10:00:00Z"));
        paperDeliveryReadyToSend3.setRequestId("req3");

        dao.insert(List.of(paperDeliveryReadyToSend2, paperDeliveryReadyToSend, paperDeliveryReadyToSend3)).block();
        List<PaperDeliveryReadyToSend> list1 = dao.getByDeliveryDate(Instant.parse("2023-10-01T10:00:00Z"));
        List<PaperDeliveryReadyToSend> list2 = dao.getByDeliveryDate(Instant.parse("2023-10-02T10:00:00Z"));

        Assertions.assertEquals(1, list1.size());
        Assertions.assertEquals(2, list2.size());
    }

}
