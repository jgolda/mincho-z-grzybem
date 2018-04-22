package com.github.serserser.kafka.generator;


import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Generator {

    public static void main(String[] args) throws IOException {
        String outputFolder = "/home/jacek/IdeaProjects/kafka-etl/src/main/resources";


        Path path = Paths.get(outputFolder);
        if ( !Files.exists(path) ) {
            System.out.println("Target directory doesn't exist");
        }

        Path inputDirectory = Paths.get("/home/jacek/IdeaProjects/kafka-etl/kafka-data-generator/src/main/resources");
        Path baseDirectory = Files.createDirectory(Paths.get(outputFolder + "/data"));

        int numberOfClients = 1_000_000;
        int numberOfShops = 100_000;
        int numberOfCommodities = 50_000;
        double maxPrice = 1000;
        int numberOfPurchases = 40_000_000;
        int maxQuantity = 20;

        List<Integer> clientIds = generateClientIds(baseDirectory, numberOfClients);
        System.out.println("Generated clients");
        List<Country> countries = generateCountries(inputDirectory, baseDirectory);
        System.out.println("Generated countries");
        ArrayList<PointOfSale> pointsOfSale = generatePointsOfSale(baseDirectory, countries, numberOfShops);
        System.out.println("Generated points of sale");
        List<Commodity> commodities = generateCommodities(baseDirectory, numberOfCommodities, maxPrice);
        System.out.println("Generated commodities");
        generatePurchases(baseDirectory, clientIds, commodities, pointsOfSale, numberOfPurchases, maxQuantity);
        System.out.println("Generated purchases");
    }

    private static void generatePurchases(Path baseDirectory, List<Integer> clientIds, List<Commodity> commodities, ArrayList<PointOfSale> pointsOfSale, int numberOfPurchases, int maxQuantity) throws IOException {
        int minClientId = clientIds.stream().mapToInt(id -> id).min().orElseThrow(() -> new IllegalStateException("Cannot find min"));
        int maxClientId = clientIds.stream().mapToInt(id -> id).max().orElseThrow(() -> new IllegalStateException("Cannot find max"));

        int minCommodityId = commodities.stream().mapToInt(c -> c.getCommodityId()).min().orElseThrow(() -> new IllegalStateException("Cannot find min"));
        int maxCommodityId = commodities.stream().mapToInt(c -> c.getCommodityId()).max().orElseThrow(() -> new IllegalStateException("Cannot find max"));

        int minPointOfSaleId = pointsOfSale.stream().mapToInt(pos -> pos.getShopId()).min().orElseThrow(() -> new IllegalStateException("Cannot find min"));
        int maxPointOfSaleId = pointsOfSale.stream().mapToInt(pos -> pos.getShopId()).max().orElseThrow(() -> new IllegalStateException("Cannot find max"));

        Path purchasesFile = Files.createFile(Paths.get(baseDirectory.toString(), "purchases.txt"));
        try ( PrintWriter commodityWriter = new PrintWriter(Files.newOutputStream(purchasesFile, StandardOpenOption.CREATE)) ) {
            for (int purchaseId = 1; purchaseId < numberOfPurchases + 1; purchaseId++) {
                int clientId = ThreadLocalRandom.current().nextInt(minClientId, maxClientId + 1);
                int commodityId = ThreadLocalRandom.current().nextInt(minCommodityId, maxCommodityId + 1);
                int quantity = ThreadLocalRandom.current().nextInt(maxQuantity);
                int pointOfSaleId = ThreadLocalRandom.current().nextInt(minPointOfSaleId, maxPointOfSaleId + 1);
                Purchase purchase = new Purchase(purchaseId, clientId, commodityId, quantity, pointOfSaleId);
                String line = String.join(",", String.valueOf(purchase.getPurchaseId()), String.valueOf(purchase.getClientId()), String.valueOf(purchase.getCommodityId()), String.valueOf(purchase.getQuantity()), String.valueOf(purchase.getPosId()));
                commodityWriter.println(line);
            }
        }
    }

    private static List<Commodity> generateCommodities(Path baseDirectory, int numberOfCommodities, double maxPrice) throws IOException {
        List<Commodity> commodities = new ArrayList<>();
        for (int commodityId = 1; commodityId < numberOfCommodities + 1; commodityId++) {
            commodities.add(new Commodity(commodityId, ThreadLocalRandom.current().nextDouble(maxPrice)));
        }

        Path commodityFile = Files.createFile(Paths.get(baseDirectory.toString(), "commodities.txt"));
        try ( PrintWriter commodityWriter = new PrintWriter(new BufferedOutputStream(Files.newOutputStream(commodityFile, StandardOpenOption.CREATE))) ) {
            for ( Commodity commodity : commodities ) {
                String line = String.join(",", String.valueOf(commodity.getCommodityId()), String.valueOf(commodity.getPrice()));
                commodityWriter.println(line);
            }
        }
        return commodities;
    }

    private static ArrayList<PointOfSale> generatePointsOfSale(Path baseDirectory, List<Country> countries, int numberOfShops) throws IOException {
        int minId = countries.stream()
                .mapToInt(Country::getId).min().orElseThrow(() -> new IllegalStateException("Failed to find min country id"));

        int maxId = countries.stream()
                .mapToInt(Country::getId).max().orElseThrow(() -> new IllegalStateException("Failed to find min country id"));

        ArrayList<PointOfSale> pointsOfSale = new ArrayList<>();
        for ( int posId = 1; posId < numberOfShops + 1; posId++ ) {
            int countryId = ThreadLocalRandom.current().nextInt(minId, maxId + 1);
            pointsOfSale.add(new PointOfSale(posId, countryId));
        }

        Path posFile = Files.createFile(Paths.get(baseDirectory.toString(), "pointsOfSale.txt"));
        try ( PrintWriter posWriter = new PrintWriter(Files.newOutputStream(posFile, StandardOpenOption.CREATE)) ) {

            for ( PointOfSale pos : pointsOfSale ) {
                String line = String.join(",", String.valueOf(pos.getShopId()), String.valueOf(pos.getCountryId()));
                posWriter.println(line);
            }
        }
        return pointsOfSale;
    }

    private static ArrayList<Country> generateCountries(Path inputDirectory, Path baseDirectory) throws IOException {
        ArrayList<Country> countries = new ArrayList<>();
        try ( Scanner reader = new Scanner(Files.newInputStream(Paths.get(inputDirectory.toString() + "/countries.txt"))) ) {
            while ( reader.hasNextLine() ) {
                String line = reader.nextLine();
                String[] record = line.split(",");
                countries.add(new Country(Integer.valueOf(record[0].trim()), record[1].trim(), record[2].trim()));
            }
        }

        Path countriesFile = Files.createFile(Paths.get(baseDirectory.toString(), "countries.txt"));
        try ( PrintWriter countriesWriter = new PrintWriter(Files.newOutputStream(countriesFile, StandardOpenOption.CREATE)) ) {

            for ( Country country : countries ) {
                String line = String.join(",", String.valueOf(country.getId()), country.getCode(), country.getName());
                countriesWriter.println(line);
            }
        }
        return countries;
    }

    private static List<Integer> generateClientIds(Path baseDirectory, int numberOfClients) throws IOException {
        int clientIdStart = 1;
        int clientIdEnd = clientIdStart + numberOfClients;

        List<Integer> clientIds = IntStream.rangeClosed(clientIdStart, clientIdEnd)
                .boxed()
                .collect(Collectors.toList());

        Path clientsFile = Files.createFile(Paths.get(baseDirectory.toString(), "clients.txt"));
        try ( PrintWriter clientWriter = new PrintWriter(Files.newOutputStream(clientsFile, StandardOpenOption.CREATE)) ) {

            for ( Integer clientId : clientIds ) {
                clientWriter.println(clientId);
            }
        }
        return clientIds;
    }

}
