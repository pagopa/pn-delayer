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
        entity.setDeliveryDriverIdGeokey("1##RM");
        entity.setDeliveryDate(paperDeliveryUtils.calculateNextWeek(Instant.now()));

        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("1", "RM",  5).block();

        Integer response = paperDeliveryDriverCapacitiesDispatchedDao.get("1", "RM").block();
        assert response != null;
        Assertions.assertEquals(5, response);

        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("1", "RM",   5).block();

        Integer response2 = paperDeliveryDriverCapacitiesDispatchedDao.get("1", "RM").block();
        assert response2 != null;
        Assertions.assertEquals(10, response2);
    }

    @Test
    void testBatchGetItem() {
        List<String> pks = List.of("test##pk1", "test##pk2", "test##pk3");
        Instant deliveryDate = paperDeliveryUtils.calculateNextWeek(Instant.now());

        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("test","pk1", 5).block();
        paperDeliveryDriverCapacitiesDispatchedDao.updateCounter("test","pk2", 5).block();

        StepVerifier.create(paperDeliveryDriverCapacitiesDispatchedDao.batchGetItem(pks, deliveryDate))
                .expectNextCount(2)
                .expectComplete()
                .verify();
    }
}
