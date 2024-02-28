package com.kvitkadev.peopledb.repository;

import com.kvitkadev.peopledb.model.Address;
import com.kvitkadev.peopledb.model.Person;
import com.kvitkadev.peopledb.model.Region;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {

    private Connection connection;
    private PeopleRepository repo;

    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("M/d/yyyy");
    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("hh:mm:ss a");

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:C:\\Users\\lkhranovska\\IdeaProjects\\Recovery\\peopledb");
        connection.setAutoCommit(false);
        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person bobby = new Person("Bobby", "Smith", ZonedDateTime.of(1982, 9, 13, 13, 13, 0, 0, ZoneId.of("-8")));
        Person savedPerson1 = repo.save(john);
        Person savedPerson2 = repo.save(bobby);
        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canSavePersonWithHomeAddress() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala", "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setHomeAddress(address);
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getHomeAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonWithBizAddress() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala", "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setBusinessAddress(address);
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getBusinessAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonWithSpouse() throws SQLException {
        Person john = new Person("John4", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person mary = new Person("Mary4", "Smith", ZonedDateTime.of(1975, 10, 10, 10, 10, 0, 0, ZoneId.of("-6")));
        john.setSpouse(mary);
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getSpouse().get().getId()).isGreaterThan(0);
//        connection.commit();
    }

    @Test
    public void canSavePersonWithChildren() throws SQLException {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        john.addChild(new Person("Johnny", "Smith", ZonedDateTime.of(2010, 01, 01, 01, 0, 0, 0, ZoneId.of("-6"))));
        john.addChild(new Person("Sarah", "Smith", ZonedDateTime.of(2012, 03, 01, 01, 0, 0, 0, ZoneId.of("-6"))));
        john.addChild(new Person("Jenny", "Smith", ZonedDateTime.of(2014, 05, 01, 01, 0, 0, 0, ZoneId.of("-6"))));

        Person savedPerson = repo.save(john);
        savedPerson.getChildren().stream()
                .map(Person::getId)
                .forEach(id -> assertThat(id).isGreaterThan(0));

//        connection.commit();
    }

    @Test
    public void canFindPersonById() {
        Person savedPerson = repo.save(new Person("test", "jackson", ZonedDateTime.now()));
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void canFindPersonByIdWithHomeAddress() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala", "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setHomeAddress(address);

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getHomeAddress().get().state()).isEqualTo("WA");
    }

    @Test
    public void canFindPersonByIdWithBizAddress() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala", "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setBusinessAddress(address);

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getBusinessAddress().get().state()).isEqualTo("WA");
    }

    @Test
    public void canFindPersonByIdWithSpouse() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person mary = new Person("Mary", "Smith", ZonedDateTime.of(1975, 10, 10, 10, 10, 0, 0, ZoneId.of("-6")));
        john.setSpouse(mary);

        Person savedPerson = repo.save(john);

        Person foundPerson = repo.findById(savedPerson.getId()).get();
        Person foundSpouse = repo.findById(foundPerson.getSpouse().get().getId()).get();

        assertThat(foundPerson.getSpouse().get().getId()).isEqualTo(foundSpouse.getId());
    }

    @Test
    public void canFindPersonByIdWithChildren() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        john.addChild(new Person("Johnny", "Smith", ZonedDateTime.of(2010, 01, 01, 01, 0, 0, 0, ZoneId.of("-6"))));
        john.addChild(new Person("Sarah", "Smith", ZonedDateTime.of(2012, 03, 01, 01, 0, 0, 0, ZoneId.of("-6"))));
        john.addChild(new Person("Jenny", "Smith", ZonedDateTime.of(2014, 05, 01, 01, 0, 0, 0, ZoneId.of("-6"))));

        Person savedPerson = repo.save(john);

        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getChildren().stream().map(Person::getFirstName).collect(toSet())).contains("Johnny", "Sarah", "Jenny");


    }

    @Test
//    @Disabled
    public void canFindAll() {
        Person p1 = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        Person p2 = repo.save( new Person("Bobby", "Smith", ZonedDateTime.of(1982, 9, 13, 13, 13, 0, 0, ZoneId.of("-8"))));
        Person p3 = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        Person p4 = repo.save( new Person("Bobby", "Smith", ZonedDateTime.of(1982, 9, 13, 13, 13, 0, 0, ZoneId.of("-8"))));
        Person p5 = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        Person p6 = repo.save( new Person("Bobby", "Smith", ZonedDateTime.of(1982, 9, 13, 13, 13, 0, 0, ZoneId.of("-8"))));
        List<Person> allPeople = repo.findAll();
        List<Person> addedPeople = List.of(p1, p2, p3, p4, p5, p6);
        assertThat(allPeople).containsAll(addedPeople);
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = repo.findById(-1L);
        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canGetCount() {
        long startCount = repo.count();
        repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save( new Person("Bobby", "Smith", ZonedDateTime.of(1982, 9, 13, 13, 13, 0, 0, ZoneId.of("-8"))));
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete() {
        Person savedPerson = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        long startCount = repo.count();
        repo.delete(savedPerson);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople() {
        Person p1 = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        Person p2 = repo.save( new Person("Bobby", "Smith", ZonedDateTime.of(1982, 9, 13, 13, 13, 0, 0, ZoneId.of("-8"))));
        long startCount = repo.count();
        repo.delete(p1, p2);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount - 2);
    }

    @Test
    public void canUpdate() {
        Person savedPerson = repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        Person p1 = repo.findById(savedPerson.getId()).get();
        savedPerson.setSalary(new BigDecimal("73000.28"));
        repo.update(savedPerson);

        Person p2 = repo.findById(savedPerson.getId()).get();

        assertThat(p2.getSalary()).isNotEqualByComparingTo(p1.getSalary());
    }


    @Test
    @Disabled
    public void loadData() throws IOException, SQLException {
        Files.lines(Path.of("C:\\Users\\lkhranovska\\Downloads\\Hr5m\\Hr5m.csv"))
//                .limit(100)
                .skip(1)
                .map(s -> s.split(","))
                .map(s -> {
                    Person person = new Person(s[2], s[4],
                            ZonedDateTime.of(
                                    LocalDateTime.of(LocalDate.parse(s[10], dateFormatter),
                                            LocalTime.parse(s[11], timeFormatter)),
                                    ZoneId.of("+0")));
                    person.setSalary(new BigDecimal(s[25]));
                    person.setEmail(s[6]);
                    return person;
                })
                .forEach(p -> repo.save(p));
        connection.commit();
    }

}
