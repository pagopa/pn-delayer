package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacitiesDispatched;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;


class PaperDeliveryDriverCapacitiesDispatchedDaoIT extends BaseTest.WithLocalStack {

    @Autowired
    PaperDeliveryDriverCapacitiesDispatchedDAO paperDeliveryDriverCapacitiesDispatchedDao;

    @Test
    void testUpdateAndGet() {
        PaperDeliveryDriverCapacitiesDispatched entity = new PaperDeliveryDriverCapacitiesDispatched();
        entity.setDeliveryDriverIdGeokey("pk");
        Instant deliveryDate = Instant.parse("2025-04-07T00:00:00Z");
        entity.setDeliveryDate(deliveryDate);

        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("pk", deliveryDate, 5).block();

        PaperDeliveryDriverCapacitiesDispatched response = paperDeliveryDriverCapacitiesDispatchedDao.get("pk", deliveryDate).block();
        assert response != null;
        Assertions.assertEquals(entity.getDeliveryDriverIdGeokey(), response.getDeliveryDriverIdGeokey());
        Assertions.assertEquals(entity.getDeliveryDate(), response.getDeliveryDate());
        Assertions.assertEquals(5, response.getUsedCapacity());

        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("pk", deliveryDate, 5).block();

        PaperDeliveryDriverCapacitiesDispatched response2 = paperDeliveryDriverCapacitiesDispatchedDao.get("pk", deliveryDate).block();
        assert response2 != null;
        Assertions.assertEquals(entity.getDeliveryDriverIdGeokey(), response2.getDeliveryDriverIdGeokey());
        Assertions.assertEquals(entity.getDeliveryDate(), response2.getDeliveryDate());
        Assertions.assertEquals(10, response2.getUsedCapacity());
    }

    @Test
    void testBatchGetItem() {
        List<String> pks = List.of("test-pk1", "test-pk2", "test-pk3");
        Instant deliveryDate = Instant.now();

        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("test-pk1", deliveryDate, 5).block();
        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("test-pk2", deliveryDate, 5).block();

        StepVerifier.create(paperDeliveryDriverCapacitiesDispatchedDao.batchGetItem(pks, deliveryDate))
                .expectNextCount(2)
                .expectComplete()
                .verify();
    }
}
