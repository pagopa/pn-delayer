package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.BaseTest;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverUsedCapacities;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

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
    void testGet() {
        Tuple2<Integer, Integer> response = paperDeliveryDriverUsedCapacitiesDAO.get("testEmpty", "RM", LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC)).block();
        Assertions.assertNull(response);
    }

    @Test
    void testUpdateAndGet() {
        PaperDeliveryDriverUsedCapacities entity = new PaperDeliveryDriverUsedCapacities();
        LocalDate dateTime = LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC);
        LocalDate nextWeek = dateTime.with(TemporalAdjusters.next(DayOfWeek.of(1)));
        entity.setUnifiedDeliveryDriverGeokey("1~RM");
        entity.setDeliveryDate(nextWeek.toString());

        paperDeliveryDriverUsedCapacitiesDAO.updateCounter("1", "RM",  5, nextWeek, 10).block();

        Tuple2<Integer, Integer> response = paperDeliveryDriverUsedCapacitiesDAO.get("1", "RM", nextWeek).block();
        assert response != null;
        Assertions.assertEquals(5, response.getT2());
        Assertions.assertEquals(10, response.getT1());

        paperDeliveryDriverUsedCapacitiesDAO.updateCounter("1", "RM",   5, nextWeek, 10).block();

        Tuple2<Integer, Integer> response2 = paperDeliveryDriverUsedCapacitiesDAO.get("1", "RM", nextWeek).block();
        assert response2 != null;
        Assertions.assertEquals(10, response2.getT2());
        Assertions.assertEquals(10, response2.getT1());
    }

    @Test
    void testBatchGetItem() {
        List<String> pks = List.of("test~pk1", "test~pk2", "test~pk3");
        LocalDate dateTime = LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC);
        LocalDate nextWeek = dateTime.with(TemporalAdjusters.next(DayOfWeek.of(1)));
        Instant deliveryDate = nextWeek.atStartOfDay().toInstant(ZoneOffset.UTC);
        LocalDate date = deliveryDate.atZone(ZoneOffset.UTC).toLocalDate();


        paperDeliveryDriverUsedCapacitiesDAO.updateCounter("test","pk1", 5, date, 10).block();
        paperDeliveryDriverUsedCapacitiesDAO.updateCounter("test","pk2", 5, date, 10).block();

        StepVerifier.create(paperDeliveryDriverUsedCapacitiesDAO.batchGetItem(pks, date))
                .expectNextCount(2)
                .expectComplete()
                .verify();
    }
}
