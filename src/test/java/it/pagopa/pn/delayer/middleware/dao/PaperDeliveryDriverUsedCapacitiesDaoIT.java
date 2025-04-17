package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverUsedCapacities;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.test.StepVerifier;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.util.List;


class PaperDeliveryDriverUsedCapacitiesDaoIT extends BaseTest.WithLocalStack {

    @Autowired
    PaperDeliveryDriverUsedCapacitiesDAO paperDeliveryDriverUsedCapacitiesDAO;

    @Test
    void testGet(){
        Integer response = paperDeliveryDriverUsedCapacitiesDAO.get("testEmpty", "RM", Instant.now()).block();
        Assertions.assertEquals(0, response);
    }

    @Test
    void testUpdateAndGet() {
        PaperDeliveryDriverUsedCapacities entity = new PaperDeliveryDriverUsedCapacities();
        LocalDate dateTime = LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC);
        LocalDate nextWeek = dateTime.with(TemporalAdjusters.next(DayOfWeek.of(1)));
        Instant deliveryDate = nextWeek.atStartOfDay().toInstant(ZoneOffset.UTC);
        entity.setUnifiedDeliveryDriverGeokey("1~RM");
        entity.setDeliveryDate(deliveryDate);

        paperDeliveryDriverUsedCapacitiesDAO.updateCounter("1", "RM",  5, deliveryDate).block();

        Integer response = paperDeliveryDriverUsedCapacitiesDAO.get("1", "RM", deliveryDate).block();
        assert response != null;
        Assertions.assertEquals(5, response);

        paperDeliveryDriverUsedCapacitiesDAO.updateCounter("1", "RM",   5, deliveryDate).block();

        Integer response2 = paperDeliveryDriverUsedCapacitiesDAO.get("1", "RM", deliveryDate).block();
        assert response2 != null;
        Assertions.assertEquals(10, response2);
    }

    @Test
    void testBatchGetItem() {
        List<String> pks = List.of("test~pk1", "test~pk2", "test~pk3");
        LocalDate dateTime = LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC);
        LocalDate nextWeek = dateTime.with(TemporalAdjusters.next(DayOfWeek.of(1)));
        Instant deliveryDate = nextWeek.atStartOfDay().toInstant(ZoneOffset.UTC);

        paperDeliveryDriverUsedCapacitiesDAO.updateCounter("test","pk1", 5, deliveryDate).block();
        paperDeliveryDriverUsedCapacitiesDAO.updateCounter("test","pk2", 5, deliveryDate).block();

        StepVerifier.create(paperDeliveryDriverUsedCapacitiesDAO.batchGetItem(pks, deliveryDate))
                .expectNextCount(2)
                .expectComplete()
                .verify();
    }
}
