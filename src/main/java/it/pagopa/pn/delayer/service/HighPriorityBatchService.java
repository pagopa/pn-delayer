package it.pagopa.pn.delayer.service;

import reactor.core.publisher.Mono;

public interface HighPriorityBatchService {

    Mono<Integer> initHighPriorityBatch(String pk);
}
