package com.example;

import com.example.dao.*;
import com.example.domain.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import org.hibernate.*;
import org.hibernate.cfg.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Objects.nonNull;

public class Main {
    private final SessionFactory sessionFactory;
//    private final RedisClient redisClient;
    private final ObjectMapper mapper;
    private final CityDAO cityDAO;
    private final CountryDAO countryDAO;

    public Main() {
        sessionFactory = prepareRelationalDB();
        cityDAO = new CityDAO(sessionFactory);
        countryDAO = new CountryDAO(sessionFactory);

//        redisClient = new prepareRedisClient();
        mapper = new ObjectMapper();
    }
    public static void main(String[] args) {
        Main main = new Main();
        List<City> allCities = main.fetchData(main);
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

    private void shutDown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
//        if (nonNull(redisClient)) {
//            redisClient.shutdown();
//        }
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
}