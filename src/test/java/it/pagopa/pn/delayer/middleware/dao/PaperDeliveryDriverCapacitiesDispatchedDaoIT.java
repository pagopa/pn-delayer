package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.middleware.dao.dynamo.PaperDeliveryDriverCapacitiesDispatchedDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacitiesDispatched;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

public class PaperDeliveryDriverCapacitiesDispatchedDaoIT extends BaseTest.WithLocalStack {

    @Autowired
    PaperDeliveryDriverCapacitiesDispatchedDAO paperDeliveryDriverCapacitiesDispatchedDao;

    @Test
    void testGet() {
        PaperDeliveryDriverCapacitiesDispatched entity = new PaperDeliveryDriverCapacitiesDispatched();
        entity.setDeliveryDriverIdGeokey("pk");
        entity.setDeliveryDate(String.valueOf(Instant.now().plusSeconds(7889400)));

        paperDeliveryDriverCapacitiesDispatchedDao.update("pk", Instant.now().plusSeconds(7889400), 5).block();

        PaperDeliveryDriverCapacitiesDispatched response = paperDeliveryDriverCapacitiesDispatchedDao.get("iun", Instant.now().plusSeconds(7889400)).block();
        assert response != null;
        Assertions.assertEquals(entity.getDeliveryDriverIdGeokey(), response.getDeliveryDriverIdGeokey());
        Assertions.assertEquals(entity.getDeliveryDate(), response.getDeliveryDate());
    }

    @Test
    void testUpdate() {
        PaperDeliveryDriverCapacitiesDispatched entity = new PaperDeliveryDriverCapacitiesDispatched();
        entity.setDeliveryDriverIdGeokey("pk");
        entity.setDeliveryDate(String.valueOf(Instant.now().plusSeconds(7889400)));

        paperDeliveryDriverCapacitiesDispatchedDao.update("pk", Instant.now().plusSeconds(7889400), 5).block();

        PaperDeliveryDriverCapacitiesDispatched response = paperDeliveryDriverCapacitiesDispatchedDao.get("iun", Instant.now().plusSeconds(7889400)).block();
        assert response != null;
        Assertions.assertEquals(entity.getDeliveryDriverIdGeokey(), response.getDeliveryDriverIdGeokey());
        Assertions.assertEquals(entity.getDeliveryDate(), response.getDeliveryDate());
    }

    @Test
    void testBatchGetItem() {
        List<String> pks = List.of("test-pk1", "test-pk2");
        Instant deliveryDate = Instant.now();

        StepVerifier.create(paperDeliveryDriverCapacitiesDispatchedDao.batchGetItem(pks, deliveryDate))
                .expectNextCount(2)
                .expectComplete()
                .verify();
    }
}
