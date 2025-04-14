package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static it.pagopa.pn.delayer.exception.PnDelayerExceptionCode.ERROR_CODE_NO_DELIVERY_DATE;

@Component
@RequiredArgsConstructor
public class PaperDeliveryUtils {

    private final PnDelayerConfigs pnDelayerConfig;

    public void enrichWithDeliveryDate(List<PaperDeliveryReadyToSend> tempItems, Integer capCapacity, Integer dispatchedCapCapacity) {
        Map<Instant, Integer> partitionedCapacity = retrieveCapacityInterval(capCapacity, pnDelayerConfig.getDeliveryDateInterval(), dispatchedCapCapacity);
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

    private Map<Instant, Integer> retrieveCapacityInterval(int weeklyCapacity, Duration duration, Integer dispatchedCapCapacity) {
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

        return updateCapacityMapWithDispatchedCapacity(capacityMap, dispatchedCapCapacity);

    }

    private Map<Instant, Integer> updateCapacityMapWithDispatchedCapacity(Map<Instant, Integer> capacityMap, Integer dispatchedCapCapacity) {
        AtomicInteger remainingCapacityAtomic = new AtomicInteger(dispatchedCapCapacity);
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

}
