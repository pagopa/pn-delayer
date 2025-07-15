package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class EvaluateSenderLimitJobServiceImpl implements EvaluateSenderLimitJobService {

    private final PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;

    @Override
    public Mono<Void> startSenderLimitJob(String province, Map<String, AttributeValue> lastEvaluatedKey, Instant startExecution) {
        return Mono.empty();
    }

    public Mono<Map<String, Integer>> calculateSenderLimit(LocalDate deliveryDate, Map<String, Integer> capacityMap, String province, List<String> paIdProductTypeTuples) {
        Map<String, Integer> senderLimitMap = new HashMap<>();
        List<String> senderLimitPkList = paIdProductTypeTuples.stream()
                .map(tuple -> tuple + "~" + province)
                .toList();
        return Flux.fromIterable(senderLimitPkList).buffer(25)
                .flatMap(senderLimitPkSubList -> paperDeliverySenderLimitDAO.retrieveSendersLimit(senderLimitPkSubList, deliveryDate)
                        .doOnNext(paperDeliverySenderLimit -> {
                            Integer limit = (capacityMap.get(paperDeliverySenderLimit.getProductType()) * paperDeliverySenderLimit.getPercentageLimit()) / 100;
                            senderLimitMap.put(paperDeliverySenderLimit.getPk(), limit);
                        }))
                .then(Mono.just(senderLimitMap));
    }
}
