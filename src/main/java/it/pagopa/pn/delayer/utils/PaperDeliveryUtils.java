package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfig;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryReadyToSend;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component
@RequiredArgsConstructor
public class PaperDeliveryUtils {

    private final PnDelayerConfig pnDelayerConfig;

    public List<PaperDeliveryReadyToSend> mapToPaperDeliveryReadyToSend(List<? extends PaperDeliveryHighPriority> items) {
        AtomicLong counter = new AtomicLong(0);
        return items.stream()
                .map(paperDeliveryHighPriority -> {
                    PaperDeliveryReadyToSend paperDeliveryReadyToSend = new PaperDeliveryReadyToSend();
                    paperDeliveryReadyToSend.setPk("PK");
                    paperDeliveryReadyToSend.setCreatedAt(Instant.now().plusNanos(counter.getAndIncrement()));
                    paperDeliveryReadyToSend.setRequestId(paperDeliveryHighPriority.getRequestId());
                    paperDeliveryReadyToSend.setIun(paperDeliveryHighPriority.getIun());
                    return paperDeliveryReadyToSend;
                })
                .toList();
    }

    public String calculateNextWeek(String createdAt) {
        LocalDate dateTime = LocalDate.ofInstant(Instant.parse(createdAt), ZoneOffset.UTC);
        LocalDate nextWeek = dateTime.with(TemporalAdjusters.next(DayOfWeek.of(pnDelayerConfig.getDeliveryDateDayOfWeek())));
        return nextWeek.atStartOfDay().toInstant(ZoneOffset.UTC).toString();
    }

}
