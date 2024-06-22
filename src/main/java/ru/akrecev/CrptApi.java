package ru.akrecev;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class CrptApi {
    private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final int requestLimit;
    private final AtomicInteger requestCount;
    private final long intervalMillis;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.requestLimit = requestLimit;
        this.requestCount = new AtomicInteger(0);
        this.intervalMillis = timeUnit.toMillis(1);
        startRateLimiter();
    }

    public static void main(String[] args) {
        try {
            CrptApi api = new CrptApi(TimeUnit.MINUTES, 10);
            api.createDocument(createDescription(), new Product[]{createProduct()});
            api.shutdown();
        } catch (Exception e) {
            log.error("Error during API call", e);
        }
    }

    private void startRateLimiter() {
        scheduler.scheduleAtFixedRate(() -> requestCount.set(0), intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public void createDocument(Description description, Product[] products) throws Exception {
        if (!waitForRateLimit()) {
            log.warn("Rate limit exceeded, skipping request");
            return;
        }

        String requestBody = objectMapper.writeValueAsString(createDocumentRequest(description, products));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(API_URL))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            log.error("Failed to create document: " + response.body());
            throw new RuntimeException("Failed to create document: " + response.body());
        } else {
            log.info("Document created successfully: " + response.body());
        }
    }

    private boolean waitForRateLimit() throws InterruptedException {
        while (true) {
            int currentCount = requestCount.incrementAndGet();
            if (currentCount <= requestLimit) {
                return true;
            }
            requestCount.decrementAndGet();
            Thread.sleep(intervalMillis);
        }
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    private static DocumentRequest createDocumentRequest(Description description, Product[] products) {
        return DocumentRequest.builder()
                .description(description)
                .docId("some_doc_id")
                .docStatus("some_doc_status")
                .docType("LP_INTRODUCE_GOODS")
                .importRequest(true)
                .ownerInn("some_owner_inn")
                .participantInn("some_participant_inn")
                .producerInn("some_producer_inn")
                .productionDate("2020-01-23")
                .productionType("some_production_type")
                .products(products)
                .regDate("2020-01-23")
                .regNumber("some_reg_number")
                .build();
    }

    private static Description createDescription() {
        return Description.builder()
                .participantInn("inn1")
                .build();
    }

    private static Product createProduct() {
        return Product.builder()
                .certificateDocument("doc1")
                .certificateDocumentDate("2020-01-23")
                .certificateDocumentNumber("num1")
                .ownerInn("owner1")
                .producerInn("producer1")
                .productionDate("2020-01-23")
                .tnvedCode("code1")
                .uitCode("uit1")
                .uituCode("uitu1")
                .build();
    }

    @Builder(toBuilder = true)
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class DocumentRequest {
        private final Description description;
        private final String docId;
        private final String docStatus;
        private final String docType;
        private final boolean importRequest;
        private final String ownerInn;
        private final String participantInn;
        private final String producerInn;
        private final String productionDate;
        private final String productionType;
        private final Product[] products;
        private final String regDate;
        private final String regNumber;
    }

    @Builder(toBuilder = true)
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Description {
        private final String participantInn;
    }

    @Builder(toBuilder = true)
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Product {
        private final String certificateDocument;
        private final String certificateDocumentDate;
        private final String certificateDocumentNumber;
        private final String ownerInn;
        private final String producerInn;
        private final String productionDate;
        private final String tnvedCode;
        private final String uitCode;
        private final String uituCode;
    }
}
