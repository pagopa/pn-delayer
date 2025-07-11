package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.util.function.Tuple2;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static it.pagopa.pn.delayer.exception.PnDelayerExceptionCode.ERROR_CODE_NO_DELIVERY_DATE;

@Component
@RequiredArgsConstructor
public class PaperDeliveryUtils {

    private final PnDelayerConfigs pnDelayerConfig;

    public Integer filterAndPrepareDeliveries(List<PaperDelivery> deliveries, Tuple2<Integer, Integer> tuple, List<PaperDelivery> deliveriesToSend) {
        List<PaperDelivery> filteredList = checkCapacityAndFilterList(tuple, deliveries);
        if (CollectionUtils.isEmpty(filteredList)) {
            return 0;
        }
        deliveriesToSend.addAll(mapItemForEvaluatePrintCapacityStep(filteredList, tuple.getT1(), tuple.getT2()));
        return filteredList.size();
    }

    public List<PaperDelivery> checkCapacityAndFilterList(Tuple2<Integer, Integer> tuple, List<PaperDelivery> paperDeliveries) {
        int remainingCapacity = tuple.getT1() - Math.max(tuple.getT2(), 0);
        return remainingCapacity <= 0 ?
                Collections.emptyList() : paperDeliveries.stream().limit(Math.min(remainingCapacity, paperDeliveries.size())).toList();
    }

    /**
     * Metodo che raggruppa i CAP di uno stesso chunk di una stessa provincia
     *
     * @param paperDelivery chunk di righe recuperate a DB di una stessa provincia
     * @return una mappa con chiave CAP e valore lista di righe aventi lo stesso CAP della chiave
     */
    public Map<String, List<PaperDelivery>> groupDeliveryOnCapAndOrderOnCreatedAt(List<PaperDelivery> paperDelivery) {
        return paperDelivery.stream()
                .collect(Collectors.groupingBy(
                        PaperDelivery::getCap,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> {
                                    list.sort(Comparator.comparing(PaperDelivery::getCreatedAt));
                                    return list;
                                }
                        )
                ));
    }

    public List<PaperDelivery> mapItemForEvaluatePrintCapacityStep(List<PaperDelivery> items, Integer capCapacity, Integer usedCapCapacity) {
        List<PaperDelivery> paperDeliveryList = items.stream()
                .peek(paperDelivery -> paperDelivery.setPk(PaperDelivery.retrieveSentWeek(paperDelivery.getPk()) + "~" + WorkflowStepEnum.EVALUATE_PRINT_CAPACITY))
                .toList();

        enrichWithDeliveryDate(paperDeliveryList, capCapacity, usedCapCapacity);
        paperDeliveryList.forEach(paperDelivery -> paperDelivery.setSk(String.join("~", String.valueOf(paperDelivery.getPriority()), paperDelivery.getDeliveryDate(), paperDelivery.getRequestId())));
        return paperDeliveryList;
    }

    /**
     * This method deals with the assignment of the deliveryDate for each shipment received in input.
     * Based on the weekly capacity, the distribution of shipments within the week is calculated based on a configurable interval (default 1d).
     * The weekly capacity is then distributed across all the calculated intervals, and based on the shipments already allocated,
     * the first valid interval is retrieved, and consequently, the first available deliveryDate for sending the shipment.
     */
    private void enrichWithDeliveryDate(List<PaperDelivery> tempItems, Integer capCapacity, Integer usedCapCapacity) {
        Map<Instant, Integer> partitionedCapacity = retrieveCapacityInterval(capCapacity, pnDelayerConfig.getDeliveryDateInterval(), usedCapCapacity);
        tempItems.forEach(shipment -> partitionedCapacity.entrySet().stream()
                .filter(entry -> entry.getValue() > 0)
                .findFirst()
                .ifPresentOrElse(entry -> {
                    shipment.setDeliveryDate(entry.getKey().toString());
                    partitionedCapacity.put(entry.getKey(), entry.getValue() - 1);
                }, () -> {
                    Instant farthestDate = partitionedCapacity.keySet()
                            .stream()
                            .max(Instant::compareTo)
                            .orElseThrow(() -> new PnInternalException("No delivery date found in capacity interval Map", ERROR_CODE_NO_DELIVERY_DATE));
                    shipment.setDeliveryDate(farthestDate.toString());
                }));
    }

/**
 * This method calculates, given the weekly capacity and the configured interval, the allocations for sending shipments.
 * (For example, if the configured interval is 1d and the total capacity is 7,
 * the method will return a map containing 7 intervals where the keys are the days of the week at midnight and the values are all equal to 1).
 */
private Map<Instant, Integer> retrieveCapacityInterval(int weeklyCapacity, Duration duration, Integer usedCapCapacity) {
    Instant weekDayStart = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(pnDelayerConfig.getDeliveryDateDayOfWeek())))
            .atStartOfDay().toInstant(ZoneOffset.UTC);

    long hoursPerInterval = duration.toHours();
    long numberOfIntervalsInAWeek = Duration.ofDays(7).toHours() / hoursPerInterval;

    int capacityPerInterval = (int) Math.ceil((double) weeklyCapacity / numberOfIntervalsInAWeek);

    Map<Instant, Integer> capacityMap = IntStream.range(0, (int) numberOfIntervalsInAWeek)
            .boxed()
            .collect(Collectors.toMap(
                    i -> weekDayStart.plus(i * hoursPerInterval, ChronoUnit.HOURS),
                    i -> capacityPerInterval,
                    (x, y) -> x,
                    TreeMap::new
            ));

    return updateCapacityMapWithUsedCapacity(capacityMap, usedCapCapacity);

}

/**
 * This method updates the capacity map with the used capacity.
 * It iterates through the capacity map and decrements the capacity based on the used capacity.
 * (For example, if the calculated allocation is 1 shipment per day and the used capacity is 2,
 * the first two intervals of the week will already be occupied, and therefore shipments will have to be allocated from the third interval onwards).
 */
private Map<Instant, Integer> updateCapacityMapWithUsedCapacity(Map<Instant, Integer> capacityMap, Integer usedCapCapacity) {
    AtomicInteger remainingCapacityAtomic = new AtomicInteger(usedCapCapacity);
    capacityMap.forEach((instant, capacity) -> remainingCapacityAtomic.getAndUpdate(currentRemaining -> {
        if (currentRemaining > 0) {
            int decrementAmount = Math.min(capacity, currentRemaining);
            capacityMap.replace(instant, capacity - decrementAmount);
            return currentRemaining - decrementAmount;
        }
        return currentRemaining;
    }));
    return capacityMap;
}

/**
 * This method calculates the start day of the delivery week based on the execution batch start date.
 */
public LocalDate calculateDeliveryWeek(Instant startExcutionBatch) {
    LocalDate startDate = startExcutionBatch.atZone(ZoneOffset.UTC).toLocalDate();
    return startDate.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(pnDelayerConfig.getDeliveryDateDayOfWeek())));
}

/**
 * This method calculates the start day of the sent week based on the deliveryWeek.
 */
public LocalDate calculateSentWeek(LocalDate deliveryWeek) {
    return deliveryWeek.minusWeeks(1);
}
}
