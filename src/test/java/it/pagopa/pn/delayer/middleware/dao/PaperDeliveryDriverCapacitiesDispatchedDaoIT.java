package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacitiesDispatched;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

class PaperDeliveryDriverCapacitiesDispatchedDaoIT extends BaseTest.WithLocalStack {

    @Autowired
    PaperDeliveryDriverCapacitiesDispatchedDAO paperDeliveryDriverCapacitiesDispatchedDao;

    @Autowired
    PaperDeliveryUtils paperDeliveryUtils;


    @Test
    void testUpdateAndGet() {
        PaperDeliveryDriverCapacitiesDispatched entity = new PaperDeliveryDriverCapacitiesDispatched();
        Instant deliveryDate = paperDeliveryUtils.calculateNextWeek(Instant.now());
        entity.setDeliveryDriverIdGeokey("1##RM");
        entity.setDeliveryDate(deliveryDate);

        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("1", "RM",  5, deliveryDate).block();

        Integer response = paperDeliveryDriverCapacitiesDispatchedDao.get("1", "RM", deliveryDate).block();
        assert response != null;
        Assertions.assertEquals(5, response);

        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("1", "RM",   5, deliveryDate).block();

        Integer response2 = paperDeliveryDriverCapacitiesDispatchedDao.get("1", "RM", deliveryDate).block();
        assert response2 != null;
        Assertions.assertEquals(10, response2);
    }

    @Test
    void testBatchGetItem() {
        List<String> pks = List.of("test##pk1", "test##pk2", "test##pk3");
        Instant deliveryDate = paperDeliveryUtils.calculateNextWeek(Instant.now());

        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("test","pk1", 5, deliveryDate).block();
        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("test","pk2", 5, deliveryDate).block();

        StepVerifier.create(paperDeliveryDriverCapacitiesDispatchedDao.batchGetItem(pks, deliveryDate))
                .expectNextCount(2)
                .expectComplete()
                .verify();
    }
}
