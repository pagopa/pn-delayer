# Copilot instructions for `pn-delayer`

## Build and test commands

### Root Spring Boot batch service

- Build the root service: `./mvnw clean package`
- Run the full Java test suite: `./mvnw test`
- Run one Java test class: `./mvnw -Dtest=EvaluateSenderLimitJobServiceTest test`
- Run one Java test method: `./mvnw -Dtest=EvaluateSenderLimitJobServiceTest#startSenderLimitJob_singleDriver_withoutLastEvaluatedKey test`
- Run one LocalStack-backed DAO integration test (Docker required): `./mvnw -Dtest=PaperDeliveryDaoIT test`

### Lambda packages under `functions/<lambda>/`

Each Lambda is a standalone Node package with its own `package.json` and lockfile. Run commands from the specific function directory, not from the repository root.

- Install dependencies: `npm install`
- Run that Lambda's unit tests: `npm test`
- Build the zip artifact: `npm run build`
- Run one Mocha test file directly: `npx mocha './src/test/eventHandler.test.js' --recursive --timeout=5000 --exit -r dotenv/config`
- Where the package defines it, run integration tests with `npm run integrazione` (for example `kinesisPaperDeliveryLambda` and `delayerToPaperChannelLambda`)

`README_TEST.md` documents the manual end-to-end flow: preloading DynamoDB data, creating `.env` files inside specific Lambda directories, and running the Spring batch job locally with the required environment variables.

## High-level architecture

This repository is not a single runtime. It has:

- a root Spring Boot 3 batch application (`src/main/java/...`) that executes one scheduling step per process run
- multiple standalone Node 20 Lambdas under `functions/` that feed data into the scheduling tables, trigger downstream workflows, or support testing/export flows

The end-to-end scheduling flow is:

1. Upstream Lambdas populate DynamoDB tables used by the planner. In particular:
   - `kinesisPaperDeliveryLambda` ingests prepare phase 1 events and writes paper-delivery records plus counters
   - `delayerReceiverOrdersSendersLambda` converts sender order files into weekly provincial sender limits and related counters
   - the other Lambdas in `functions/` handle surrounding orchestration, cancellation, export, and test support
2. The Spring app starts at `DelayerApplication`, but the real entrypoint is `PaperDeliveryJobRunner`, a `CommandLineRunner` active outside the `test` profile.
3. `PaperDeliveryJobRunner` dispatches exactly one batch step based on `pn.delayer.workflow-step` / `PN_DELAYER_WORKFLOWSTEP`:
   - `EVALUATE_SENDER_LIMIT`
   - `EVALUATE_DRIVER_CAPACITY`
   - `EVALUATE_RESIDUAL_CAPACITY`
4. `EvaluateSenderLimitJobServiceImpl` enriches deliveries with priority and unified driver information, excludes RS and second attempts from sender-limit checks, evaluates sender quotas, and writes new `PaperDelivery` rows for the next workflow step.
5. `EvaluateDriverCapacityJobServiceImpl` and `EvaluateResidualCapacityJobServiceImpl` are thin wrappers over `PaperDeliveryUtils`, which reads the relevant deliveries, evaluates province/CAP residual capacity, updates used-capacity and print counters, and pushes overflow back to next week's `EVALUATE_SENDER_LIMIT`.
6. `delayerToPaperChannelLambda` is the downstream consumer of `EVALUATE_PRINT_CAPACITY`: it sends printable items toward paper channel phase 2 and postpones print-capacity overflow to the following week.

Within the Java code, the main layering is:

- `service/`: thin step-specific entrypoints
- `utils/`: most scheduling logic lives here (`PaperDeliveryUtils`, `PnDelayerUtils`, `DeliveryDriverUtils`, `SenderLimitUtils`, `PrintCapacityUtils`)
- `middleware/dao/dynamo/`: async DynamoDB access using the AWS SDK enhanced client
- `middleware/dao/dynamo/entity/`: table entities and key-building rules

## Key conventions

- Treat the root app as a batch worker, not as an HTTP service. Most changes belong in the step services, utilities, or DynamoDB DAOs.
- Preserve the `PaperDelivery` transition model. Moving a delivery to a new workflow step is done by creating a new `PaperDelivery` from an existing one, usually through the mapping helpers in `PnDelayerUtils` or the `PaperDelivery(PaperDelivery, WorkflowStepEnum, LocalDate)` constructor.
- DynamoDB key shape is part of the algorithm:
  - `pk` is always `<deliveryWeek>~<workflowStep>`
  - `sk` ordering changes by step:
    - sender limit: `province~date~requestId`
    - driver capacity: `unifiedDeliveryDriver~province~priority~date~requestId`
    - residual capacity: `unifiedDeliveryDriver~province~date~requestId`
    - print capacity: `priority~date~requestId`
  DAO queries rely on `sortBeginsWith`, so changing key prefixes changes processing order and query behavior.
- `deliveryWeek` is normalized by `PnDelayerUtils.calculateDeliveryWeek()` using `pn.delayer.delivery-date-day-of-week`. "Send to next week" means recreating the item in `EVALUATE_SENDER_LIMIT` with `deliveryWeek.plusWeeks(1)`.
- RS shipments and second attempts (`attempt == 1`) bypass sender-limit evaluation and go straight to driver-capacity handling. Their sort-key date comes from `prepareRequestDate`; other shipments use `notificationSentAt`.
- Driver lookup is two-stage in `DeliveryDriverUtils`: first an in-memory one-hour cache keyed by `cap~productType`, then the Paper Channel Tender API Lambda. Priority is assigned from the SSM parameter map referenced by `pn.delayer.paper-delivery-priority-parameter-name`.
- Driver and residual batches depend on `AWS_BATCH_JOB_ARRAY_INDEX`: the config property contains a JSON array of provinces, and the batch array index chooses the province processed by the current worker.
- Print capacity configuration is a list of `YYYY-MM-DD;capacity` entries (`pn.delayer.print-capacity`). `PrintCapacityUtils` sorts them descending by start date and uses the latest entry valid for the current delivery week.
- Java tests use JUnit 5, Mockito, and Reactor `StepVerifier`. DAO integration tests extend `BaseTest.WithLocalStack`, which boots LocalStack DynamoDB and seeds tables through `src/test/resources/testcontainers/init.sh` and `application-test.properties`.
- When working on Lambdas under `functions/`, keep changes scoped to the individual package. The Lambda packages are not managed from a shared workspace-level Node toolchain.
