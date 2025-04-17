package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import it.pagopa.pn.delayer.model.PaperDeliveryTransactionRequest;
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


    public Integer filterAndPrepareDeliveries(List<PaperDeliveryHighPriority> deliveries, PaperDeliveryTransactionRequest transactionRequest, Tuple2<Integer, Integer> tuple) {
        List<PaperDeliveryHighPriority> filteredList = checkCapacityAndFilterList(tuple, deliveries);
        if (CollectionUtils.isEmpty(filteredList)) {
            return 0;
        }
        transactionRequest.getPaperDeliveryHighPriorityList().addAll(filteredList);
        transactionRequest.getPaperDeliveryReadyToSendList().addAll(mapToPaperDeliveryReadyToSend(filteredList, tuple.getT1(), tuple.getT2()));
        return filteredList.size();
    }

    public List<PaperDeliveryHighPriority> checkCapacityAndFilterList(Tuple2<Integer, Integer> tuple, List<PaperDeliveryHighPriority> paperDeliveryHighPriorities) {
        int remainingCapacity = tuple.getT1() - Math.max(tuple.getT2(), 0);
        return remainingCapacity == 0 ?
                Collections.emptyList() : paperDeliveryHighPriorities.stream().limit(Math.min(remainingCapacity, paperDeliveryHighPriorities.size())).toList();
    }

    public boolean checkListsSize(PaperDeliveryTransactionRequest transactionRequest) {
        return !CollectionUtils.isEmpty(transactionRequest.getPaperDeliveryHighPriorityList()) &&
                !CollectionUtils.isEmpty(transactionRequest.getPaperDeliveryReadyToSendList()) &&
                transactionRequest.getPaperDeliveryReadyToSendList().size() == transactionRequest.getPaperDeliveryHighPriorityList().size();
    }

    public Map<String, List<PaperDeliveryHighPriority>> groupDeliveryOnCapAndOrderOnCreatedAt(List<PaperDeliveryHighPriority> paperDeliveryHighPriorities) {
        return paperDeliveryHighPriorities.stream()
                .collect(Collectors.groupingBy(
                        PaperDeliveryHighPriority::getCap,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> {
                                    list.sort(Comparator.comparing(PaperDeliveryHighPriority::getCreatedAt));
                                    return list;
                                }
                        )
                ));
    }
    private List<PaperDeliveryReadyToSend> mapToPaperDeliveryReadyToSend(List<PaperDeliveryHighPriority> items, Integer capCapacity, Integer usedCapCapacity) {
        List<PaperDeliveryReadyToSend> paperDeliveryReadyToSendList = items.stream()
                .map(paperDeliveryHighPriority -> {
                    PaperDeliveryReadyToSend paperDeliveryReadyToSend = new PaperDeliveryReadyToSend();
                    paperDeliveryReadyToSend.setRequestId(paperDeliveryHighPriority.getRequestId());
                    paperDeliveryReadyToSend.setIun(paperDeliveryHighPriority.getIun());
                    return paperDeliveryReadyToSend;
                })
                .toList();

        enrichWithDeliveryDate(paperDeliveryReadyToSendList, capCapacity, usedCapCapacity);

        return paperDeliveryReadyToSendList;
    }

    /**
     This method deals with the assignment of the deliveryDate for each shipment received in input.
     Based on the weekly capacity, the distribution of shipments within the week is calculated based on a configurable interval (default 1d).
     The weekly capacity is then distributed across all the calculated intervals, and based on the shipments already allocated,
     the first valid interval is retrieved, and consequently, the first available deliveryDate for sending the shipment.
     */
    private void enrichWithDeliveryDate(List<PaperDeliveryReadyToSend> tempItems, Integer capCapacity, Integer usedCapCapacity) {
        Map<Instant, Integer> partitionedCapacity = retrieveCapacityInterval(capCapacity, pnDelayerConfig.getDeliveryDateInterval(), usedCapCapacity);
        tempItems.forEach(shipment -> partitionedCapacity.entrySet().stream()
                .filter(entry -> entry.getValue() > 0)
                .findFirst()
                .ifPresentOrElse(entry -> {
                    shipment.setDeliveryDate(entry.getKey());
                    partitionedCapacity.put(entry.getKey(), entry.getValue() - 1);
                }, () -> {
                    Instant farthestDate = partitionedCapacity.keySet()
                            .stream()
                            .max(Instant::compareTo)
                            .orElseThrow(() -> new PnInternalException("No delivery date found in capacity interval Map", ERROR_CODE_NO_DELIVERY_DATE));
                    shipment.setDeliveryDate(farthestDate);
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
     * This method calculates the next delivery week based on the configured day of the week.
     * It takes the createdAt timestamp and finds the next occurrence of the specified day of the week.
     */
    public Instant calculateNextWeek(Instant createdAt) {
        LocalDate dateTime = LocalDate.ofInstant(createdAt, ZoneOffset.UTC);
        LocalDate nextWeek = dateTime.with(TemporalAdjusters.next(DayOfWeek.of(pnDelayerConfig.getDeliveryDateDayOfWeek())));
        return nextWeek.atStartOfDay().toInstant(ZoneOffset.UTC);
    }

}
