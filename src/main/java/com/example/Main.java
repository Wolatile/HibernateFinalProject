package com.example;

import com.example.dao.*;
import com.example.domain.*;
import com.example.redis.CityCountry;
import com.example.redis.Language;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.hibernate.*;
import org.hibernate.cfg.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class Main {
    private final SessionFactory sessionFactory;
    private final RedisClient redisClient;
    private final ObjectMapper mapper;
    private final CityDAO cityDAO;
    private final CountryDAO countryDAO;

    public Main() {
        sessionFactory = prepareRelationalDB();
        cityDAO = new CityDAO(sessionFactory);
        countryDAO = new CountryDAO(sessionFactory);

        redisClient = prepareRedisClient();
        mapper = new ObjectMapper();
    }
    public static void main(String[] args) {
        Main main = new Main();
        List<City> allCities = main.fetchData(main);
        List<CityCountry> preparedData = main.transformData(allCities);
        main.pushToRedis(preparedData);

        main.sessionFactory.getCurrentSession().close();

        List<Integer> ids = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

        long startRedis = System.currentTimeMillis();
        main.testRedisData(ids);
        long stopRedis = System.currentTimeMillis();

        long startMysql = System.currentTimeMillis();
        main.testMysqlData(ids);
        long stopMysql = System.currentTimeMillis();

        System.out.printf("%s:\t%d ms\n", "Redis", (stopRedis - startRedis));
        System.out.printf("%s:\t%d ms\n", "MySQL", (stopMysql - startMysql));

        main.shutDown();
    }

    private SessionFactory prepareRelationalDB() {
        final SessionFactory sessionFactory;
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3306/world");
        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "root");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "validate");
        properties.put(Environment.STATEMENT_BATCH_SIZE, "100");

        sessionFactory = new Configuration()
                .addAnnotatedClass(City.class)
                .addAnnotatedClass(Country.class)
                .addAnnotatedClass(CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();
        return sessionFactory;
    }

    private RedisClient prepareRedisClient() {
        RedisClient redisClient = RedisClient.create(RedisURI.create("localhost", 6379));
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            System.out.println("\nConnected to Redis\n");
        }
        return redisClient;
    }

    private void pushToRedis(List<CityCountry> data) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();
            for (CityCountry cityCountry : data) {
                try {
                    sync.set(String.valueOf(cityCountry.getId()), mapper.writeValueAsString(cityCountry));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void shutDown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
        if (nonNull(redisClient)) {
            redisClient.shutdown();
        }
    }

    private List<City> fetchData(Main main) {
        try (Session session = main.sessionFactory.getCurrentSession()) {
            List<City> allCities = new ArrayList<>();
            session.beginTransaction();

            List<Country> countries  = main.countryDAO.getAll();
            int totalCount = main.cityDAO.getTotalCount();
            int step = 500;
            for (int i = 0; i < totalCount; i += step) {
                allCities.addAll(main.cityDAO.getItems(i, step));
            }
            session.getTransaction().commit();
            return allCities;
        }
    }

    private List<CityCountry> transformData(List<City> cities) {
        return cities.stream().map(city -> {
            CityCountry cityCountry = new CityCountry();
            cityCountry.setId(city.getId());
            cityCountry.setName(city.getName());
            cityCountry.setDistrict(city.getDistrict());
            cityCountry.setPopulation(city.getPopulation());

            Country country = city.getCountry();
            cityCountry.setCountryCode(country.getCode());
            cityCountry.setAlternativeCountryCode(country.getAlternativeCode());
            cityCountry.setCountryName(country.getName());
            cityCountry.setContinent(country.getContinent());
            cityCountry.setCountryRegion(country.getRegion());
            cityCountry.setCountrySurfaceArea(country.getSurfaceArea());
            cityCountry.setCountryPopulation(country.getPopulation());
            Set<CountryLanguage> countryLanguages = country.getLanguages();
            Set<Language> languages = countryLanguages.stream().map(countryLanguage -> {
                Language language = new Language();
                language.setLanguage(countryLanguage.getLanguage());
                language.setOfficial(countryLanguage.getOfficial());
                language.setPercentage(countryLanguage.getPercentage());
                return language;
            }).collect(Collectors.toSet());
            cityCountry.setLanguages(languages);

            return cityCountry;
        }).collect(Collectors.toList());
    }

    private void testRedisData(List<Integer> ids) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();
            for (Integer id : ids) {
                String value = sync.get(String.valueOf(id));
                try {
                    mapper.readValue(value, CityCountry.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void testMysqlData(List<Integer> ids) {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            for (Integer id : ids) {
                City city = cityDAO.getById(id);
                Set<CountryLanguage> languages = city.getCountry().getLanguages();
            }
            session.getTransaction().commit();
        }
    }
}