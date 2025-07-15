package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryCounterDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryDriverCapacitiesDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryDriverCapacity;
import it.pagopa.pn.delayer.model.DriversTotalCapacity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.List;

@ExtendWith(MockitoExtension.class)
public class DeliveryDriverCapacityServiceTest {

    @InjectMocks
    private DeliveryDriverCapacityService deliveryDriverCapacityService;

    @Mock
    private PaperDeliveryCounterDAO paperDeliveryCounterDAO;

    @Mock
    private PaperDeliveryDriverCapacitiesDAO paperDeliveryDriverCapacitiesDAO;

    @Test
    void retrieveDriversCapacityForProductOnProvinceReturnsCorrectCapacityAndDrivers() {
        String tenderId = "tenderId";
        String province = "province";
        LocalDate deliveryDate = LocalDate.now().minusWeeks(1).with(DayOfWeek.MONDAY);
        String sk = "EXCLUDE~" + province;

        PaperDeliveryCounter paperDeliveryCounter = new PaperDeliveryCounter();
        paperDeliveryCounter.setCounter(5);

        PaperDeliveryDriverCapacity paperDeliveryDriverCapacity1 = new PaperDeliveryDriverCapacity();
        paperDeliveryDriverCapacity1.setUnifiedDeliveryDriver("driver1");
        paperDeliveryDriverCapacity1.setCapacity(10);

        PaperDeliveryDriverCapacity paperDeliveryDriverCapacity2 = new PaperDeliveryDriverCapacity();
        paperDeliveryDriverCapacity2.setUnifiedDeliveryDriver("driver2");
        paperDeliveryDriverCapacity2.setCapacity(10);

        Mockito.when(paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, sk))
                .thenReturn(Mono.just(paperDeliveryCounter));

        Mockito.when(paperDeliveryDriverCapacitiesDAO.retrieveUnifiedDeliveryDriversOnProvince(tenderId, province, deliveryDate))
                .thenReturn(Mono.just(List.of(paperDeliveryDriverCapacity1, paperDeliveryDriverCapacity2)));

        DriversTotalCapacity result = deliveryDriverCapacityService.retrieveDriversCapacityOnProvince(deliveryDate, tenderId, province).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(15, result.getCapacity());
        Assertions.assertEquals(List.of("driver1", "driver2"), result.getUnifiedDeliveryDrivers());
    }

    @Test
    void retrieveDriversCapacityForProductOnProvinceHandlesOneDriverCapacities() {
        String tenderId = "tenderId";
        String province = "province";
        LocalDate deliveryDate = LocalDate.now().minusWeeks(1).with(DayOfWeek.MONDAY);
        String sk = "EXCLUDE~" + province;

        PaperDeliveryCounter paperDeliveryCounter = new PaperDeliveryCounter();
        paperDeliveryCounter.setCounter(5);

        PaperDeliveryDriverCapacity paperDeliveryDriverCapacity1 = new PaperDeliveryDriverCapacity();
        paperDeliveryDriverCapacity1.setUnifiedDeliveryDriver("driver1");
        paperDeliveryDriverCapacity1.setCapacity(10);

        Mockito.when(paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, sk))
                .thenReturn(Mono.just(paperDeliveryCounter));

        Mockito.when(paperDeliveryDriverCapacitiesDAO.retrieveUnifiedDeliveryDriversOnProvince(tenderId, province, deliveryDate))
                .thenReturn(Mono.just(List.of(paperDeliveryDriverCapacity1)));

        DriversTotalCapacity result = deliveryDriverCapacityService.retrieveDriversCapacityOnProvince(deliveryDate, tenderId, province).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getCapacity());
        Assertions.assertEquals("driver1", result.getUnifiedDeliveryDrivers().getFirst());
    }

    @Test
    void retrieveDriversCapacityForProductOnProvinceHandlesNullCounter() {
        String tenderId = "tenderId";
        String province = "province";
        LocalDate deliveryDate = LocalDate.now().minusWeeks(1).with(DayOfWeek.MONDAY);
        String sk = "EXCLUDE~" + province;

        PaperDeliveryDriverCapacity paperDeliveryDriverCapacity1 = new PaperDeliveryDriverCapacity();
        paperDeliveryDriverCapacity1.setUnifiedDeliveryDriver("driver1");
        paperDeliveryDriverCapacity1.setCapacity(10);

        PaperDeliveryDriverCapacity paperDeliveryDriverCapacity2 = new PaperDeliveryDriverCapacity();
        paperDeliveryDriverCapacity2.setUnifiedDeliveryDriver("driver2");
        paperDeliveryDriverCapacity2.setCapacity(10);

        Mockito.when(paperDeliveryCounterDAO.getPaperDeliveryCounter(deliveryDate, sk))
                .thenReturn(Mono.empty());
        Mockito.when(paperDeliveryDriverCapacitiesDAO.retrieveUnifiedDeliveryDriversOnProvince(tenderId, province, deliveryDate))
                .thenReturn(Mono.just(List.of(paperDeliveryDriverCapacity1, paperDeliveryDriverCapacity2)));

        DriversTotalCapacity result = deliveryDriverCapacityService.retrieveDriversCapacityOnProvince(deliveryDate, tenderId, province).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(20, result.getCapacity());
        Assertions.assertEquals(List.of("driver1", "driver2"), result.getUnifiedDeliveryDrivers());
    }
}
