package it.pagopa.pn.delayer.middleware.dao;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryCounter;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public interface PaperDeliveryCounterDAO {

    Mono<List<PaperDeliveryCounter>> getPaperDeliveryCounter(String pk, String sk, Integer limit);

    Mono<Void> updatePrintCapacityCounter(LocalDate deliveryDate, Integer counter, Integer weeklyPrintCapacity);

    Mono<Void> updateExcludeCounter(LocalDate deliveryWeek, Map<String, Long> excludeGroupedRecords);


}
