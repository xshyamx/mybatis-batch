package com.example;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    static void createTable(Connection c)  {
        try (
                Statement s = c.createStatement()
        ) {
            s.execute("create table person (" +
                    "person_id int auto_increment," +
                    "first_name varchar(50)," +
                    "last_name varchar(50)," +
                    "occupation varchar(50)," +
                    "primary key (person_id)" +
                    ")");
            try ( ResultSet rs = s.executeQuery("select count(*) from person") ) {
                if ( rs.next() ) {
                    System.out.println("create table person count " + rs.getInt(1));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    static void dropTable(Connection c)  {
        try (
                Statement s = c.createStatement()
        ) {
            try ( ResultSet rs = s.executeQuery("select count(*) from person") ) {
                if ( rs.next() ) {
                    System.out.println("drop table person count " + rs.getInt(1));
                }
            }
            s.execute("drop table person");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static Person[] getPersons(final int n) {
        Person[] persons = new Person[n];
        for (int i = 0; i < n; i++) {
            String suffix = String.format("-%05d", i);
            persons[i] = new Person("first" +suffix, "last"+suffix, "job"+suffix);
        }
        return persons;
    }
    static Person[] getPersons() {
        return new Person[]{
                new Person("John", "Doe", "Tinker"),
                new Person("Jane", "Doe", "Tailor"),
                new Person("John", "Wick", "Soldier"),
                new Person("Jane", "Wick", "Spy")
        };
    }
    public static void main(String[] args) throws IOException {
        System.out.println(Arrays.toString(args));
//        runTest(100);
//        if ( true ) return;
        int iterations = 10;
        if ( args.length > 0 ) {
            try {
                iterations = Integer.parseInt(args[0]);
            }catch (NumberFormatException nfe) {
                iterations = 10;
            }
        }
        final int[] counts = new int[] {10, 100, 500, 1000, 5000, 10000 };
        int[][] rows = new int[counts.length][4];
        try (FileWriter w = new FileWriter("target/raw.csv")) {
            w.write("iteration,count,mybatis,mybatis-loop,batch\n");
            for (int j = 0; j < iterations; j++) {
                for (int i = 0; i < counts.length; i++) {
                    int[] r = runTest(counts[i]);
                    rows[i][0] = counts[i];
                    rows[i][1] += r[1];
                    rows[i][2] += r[2];
                    rows[i][3] += r[3];
                    w.write(String.format("%d,%d,%d,%d,%d\n", j, r[0], r[1], r[2], r[3]));
                }
            }
        } finally {
            System.out.println("Wrote raw.csv, iterations=" + iterations);
        }

        int[][] avg = new int[counts.length][4];
        try (FileWriter w = new FileWriter("target/out.csv")) {
            w.write("count,mybatis,mybatis-loop,batch\n");
            for (int i = 0; i < rows.length; i++) {
                int[] row = rows[i];
                for (int j = 0; j < row.length; j++) {
                    if ( j == 0 ) {
                        avg[i][j] = row[j];
                    } else {
                        avg[i][j] = row[j]/iterations;
                    }
                }
                w.write(String.format("'%d,%d,%d,%d\n", avg[i][0], avg[i][1], avg[i][2], avg[i][3]));

            }
        } finally {
            System.out.println("Wrote out.csv");
        }
        Map<String, int[]> datasets = new HashMap<>();
        String[] labels = new String[]{"mybatis", "mybatis-loop", "jdbc-batch"};
        for ( String label : labels ) {
            datasets.put(label, new int[counts.length]);
        }
        String prefix = "{ type: 'bar', data: { labels: ['10', '100', '500', '1000', '5000', '10000'], datasets: [";
        for (int i = 0; i < avg.length; i++) {
            datasets.get("mybatis")[i] = avg[i][1];
            datasets.get("mybatis-loop")[i] = avg[i][2];
            datasets.get("jdbc-batch")[i] = avg[i][3];
        }
        List<String> items = Arrays.stream(labels).map((String k) -> {
            return String.format("{ label: '%s', data: %s }", k, Arrays.toString(datasets.get(k)));
        }).collect(Collectors.toList());

        String suffix = String.format("] }, options: { title: { display: true, text: 'Batch Size vs Time Taken\\n(Avg. of %d iterations in ms)' } } }", iterations);
        URI u = URI.create("https://quickchart.io/chart?c=" + URLEncoder.encode(prefix + String.join(",", items) + suffix, "utf-8"));
        System.out.println(u);
    }
    static int[] runTest(int count) throws IOException {
        String resource = "com/example/mybatis-config.xml";
        Properties props = new Properties();
        props.put("driver", "org.h2.Driver");
        props.put("url", "jdbc:h2:mem:test");
        int[] result = new int[4];
        int ridx = 0;
        result[ridx++] = count;
        InputStream inputStream = Resources.getResourceAsStream(resource);
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder()
                .build(inputStream, props);
        try ( SqlSession s = sqlSessionFactory.openSession() ) {
            createTable(s.getConnection());

            int lastId = -1;
            long start = System.currentTimeMillis();
            for ( Person p : getPersons(count)) {
                int n = s.insert("insertPerson", p);
                if ( n > 0 ) {
//                    System.out.println(p);
                    lastId = p.getId();
                }
            }
            result[ridx++] = (int) (System.currentTimeMillis()-start);
            System.out.printf("Inserted %d records in %dms\n", count, result[1]);
            {
                Person p = s.selectOne("selectPerson", lastId);
                System.out.println("Retrieved again " + p);
            }
            // LOOP
            System.out.println("===== LOOP =====");
            start = System.currentTimeMillis();
            int br = s.insert("insertPersons", getPersons(count));
            result[ridx++] = (int) (System.currentTimeMillis()-start);
            System.out.printf("Inserted %d records in loop %dms\n", br, result[2]);

            System.out.println("===== BATCH =====");
            // batch
            start = System.currentTimeMillis();
            String sql = "insert into\n" +
                    "          person (first_name, last_name, occupation)\n" +
                    "          values (?, ?, ?)";
            try (PreparedStatement ps = s.getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) ) {
                for ( Person p : getPersons(count) ) {
                    int idx = 1;
                    ps.setString(idx++, p.getFirstName());
                    ps.setString(idx++, p.getLastName());
                    ps.setString(idx++, p.getOccupation());
                    ps.addBatch();
                }
                int[] rets = ps.executeBatch();
//                System.out.println(Arrays.toString(rets));
                try ( ResultSet rs = ps.getGeneratedKeys() ) {
                    while ( rs.next() ) {
//                        System.out.println("Person(id=" + rs.getInt(1) + ")");
                        lastId = rs.getInt(1);
                    }
                }
                {
                    Person p = s.selectOne("selectPerson", lastId);
                    System.out.println("Retrieved again " + p);
                }
//                Collector c = new Collector();
//                s.select("getAll", c);
//                System.out.println(c.personList);
                dropTable(s.getConnection());
            } catch (SQLException e) {
                e.printStackTrace();
            }
            result[ridx++] = (int) (System.currentTimeMillis()-start);
            System.out.printf("Inserted %d records in batch %dms\n", count, result[3]);
        }
        return result;
    }
    static class Collector implements ResultHandler<Map<String, Object>> {

        List<Person> personList = new ArrayList<>();
        @Override
        public void handleResult(ResultContext<? extends Map<String, Object>> resultContext) {
            personList.add(fromMap(resultContext.getResultObject()));
        }
    }
    static Person fromMap(Map<String, Object> m) {
        Person p = new Person((String) m.get("FIRST_NAME"), (String) m.get("LAST_NAME"), (String) m.get("OCCUPATION"));
        p.setId((Integer) m.get("PERSON_ID"));
        return p;
    }
}
