package com.smallworld;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class TransactionDataFetcher {

    // Define a field to store the parsed JSON data (assuming it's available as a file)
    private List<Map<String, Object>> transactionData;

    public TransactionDataFetcher() throws FileNotFoundException {

        // Load and parse the JSON data into the 'transactionData' field in the constructor.
        try {
            InputStream inputStream = getClass().getResourceAsStream(
                        "/transactions.json");
            ObjectMapper objectMapper = new ObjectMapper();
            transactionData = objectMapper.readValue(inputStream, new TypeReference<List<Map<String, Object>>>() {});
        } catch (IOException e) {
            // Handle the exception (e.g., log or throw a custom exception)
            e.printStackTrace();
        }
    }

    public double getTotalTransactionAmount() {
        return transactionData.stream()
                .mapToDouble(transaction -> (double) transaction.get("amount"))
                .sum();
    }

    public double getTotalTransactionAmountSentBy(String senderFullName) {
        return transactionData.stream()
                .filter(transaction -> senderFullName.equals(transaction.get("senderFullName")))
                .mapToDouble(transaction -> (double) transaction.get("amount"))
                .sum();
    }

    public double getMaxTransactionAmount() {
        return transactionData.stream()
                .mapToDouble(transaction -> (double) transaction.get("amount"))
                .max()
                .orElse(0.0);
    }

    public long countUniqueClients() {
        Set<String> uniqueClients = new HashSet<>();
        transactionData.forEach(transaction -> {
            uniqueClients.add((String) transaction.get("senderFullName"));
            uniqueClients.add((String) transaction.get("beneficiaryFullName"));
        });
        return uniqueClients.size();
    }

    public boolean hasOpenComplianceIssues(String clientFullName) {
        return transactionData.stream()
                .anyMatch(transaction ->
                        clientFullName.equals(transaction.get("senderFullName"))
                                || clientFullName.equals(transaction.get("beneficiaryFullName"))
                                && !Boolean.TRUE.equals(transaction.get("issueSolved")));
    }

    public Map<String, List<Map<String, Object>>> getTransactionsByBeneficiaryName() {
        return transactionData.stream()
                .collect(Collectors.groupingBy(transaction -> (String) transaction.get("beneficiaryFullName")));
    }

    public Set<Integer> getUnsolvedIssueIds() {
        return transactionData.stream()
                .filter(transaction -> !Boolean.TRUE.equals(transaction.get("issueSolved")))
                .map(transaction -> (Integer) transaction.get("issueId"))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    public List<String> getAllSolvedIssueMessages() {
        return transactionData.stream()
                .filter(transaction -> Boolean.TRUE.equals(transaction.get("issueSolved")))
                .map(transaction -> (String) transaction.get("issueMessage"))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> getTop3TransactionsByAmount() {
        return transactionData.stream()
                .sorted(Comparator.comparingDouble(transaction -> -(double) transaction.get("amount")))
                .limit(3)
                .collect(Collectors.toList());
    }

    public Optional<String> getTopSender() {
        Map<String, Double> senderAmountMap = new HashMap<>();
        transactionData.forEach(transaction -> {
            String sender = (String) transaction.get("senderFullName");
            double amount = (double) transaction.get("amount");
            senderAmountMap.put(sender, senderAmountMap.getOrDefault(sender, 0.0) + amount);
        });

        return senderAmountMap.entrySet().stream()
                .max(Comparator.comparingDouble(Map.Entry::getValue))
                .map(Map.Entry::getKey);
    }

    // Example usage in your application:
    public static void main(String[] args) throws FileNotFoundException {
        TransactionDataFetcher dataFetcher = new TransactionDataFetcher();

        double totalAmount = dataFetcher.getTotalTransactionAmount();
        System.out.println("Total Transaction Amount: " + totalAmount);

        double amountSentByTomShelby = dataFetcher.getTotalTransactionAmountSentBy("Tom Shelby");
        System.out.println("Total Amount Sent by Tom Shelby: " + amountSentByTomShelby);

        double maxTransactionAmount = dataFetcher.getMaxTransactionAmount();
        System.out.println("Max Transaction Amount: " + maxTransactionAmount);

        long uniqueClientsCount = dataFetcher.countUniqueClients();
        System.out.println("Unique Clients Count: " + uniqueClientsCount);

        boolean hasOpenIssues = dataFetcher.hasOpenComplianceIssues("Tom Shelby");
        System.out.println("Tom Shelby has open compliance issues: " + hasOpenIssues);

        Map<String, List<Map<String, Object>>> transactionsByBeneficiary = dataFetcher.getTransactionsByBeneficiaryName();
        System.out.println("Transactions by Beneficiary: " + transactionsByBeneficiary);

        Set<Integer> unsolvedIssueIds = dataFetcher.getUnsolvedIssueIds();
        System.out.println("Unsolved Issue IDs: " + unsolvedIssueIds);

        List<String> solvedIssueMessages = dataFetcher.getAllSolvedIssueMessages();
        System.out.println("Solved Issue Messages: " + solvedIssueMessages);

        List<Map<String, Object>> top3TransactionsByAmount = dataFetcher.getTop3TransactionsByAmount();
        System.out.println("Top 3 Transactions by Amount: " + top3TransactionsByAmount);

        Optional<String> topSender = dataFetcher.getTopSender();
        System.out.println("Top Sender: " + topSender.orElse("No sender found"));
    }
}
