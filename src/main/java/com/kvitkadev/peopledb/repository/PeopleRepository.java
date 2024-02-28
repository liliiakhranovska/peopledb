package com.kvitkadev.peopledb.repository;

import com.kvitkadev.peopledb.annotation.SQL;
import com.kvitkadev.peopledb.model.Address;
import com.kvitkadev.peopledb.model.CrudOperation;
import com.kvitkadev.peopledb.model.Person;
import com.kvitkadev.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class PeopleRepository extends CrudRepository<Person> {
    private AddressRepository addressRepository = null;
    private static final String SAVE_PERSON_SQL = """
        INSERT INTO PEOPLE 
        (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BIZ_ADDRESS, SPOUSE_ID, PARENT_ID)
         VALUES (?,?,?,?,?,?,?,?,?)
    """;
    private static final String FIND_BY_ID_SQL = """
            SELECT
            PARENT.ID AS PARENT_ID, PARENT.FIRST_NAME AS PARENT_FIRST_NAME, PARENT.LAST_NAME AS PARENT_LAST_NAME,
            PARENT.DOB AS PARENT_DOB, PARENT.SALARY AS PARENT_SALARY, PARENT.EMAIL AS PARENT_EMAIL,
            CHILD.ID AS CHILD_ID, CHILD.FIRST_NAME AS CHILD_FIRST_NAME, CHILD.LAST_NAME AS CHILD_LAST_NAME,
            CHILD.DOB AS CHILD_DOB, CHILD.SALARY AS CHILD_SALARY, CHILD.EMAIL AS CHILD_EMAIL,
            SPOUSE.ID AS SPOUSE_ID, SPOUSE.FIRST_NAME AS SPOUSE_FIRST_NAME, SPOUSE.LAST_NAME AS SPOUSE_LAST_NAME, 
            SPOUSE.DOB AS SPOUSE_DOB, SPOUSE.SALARY AS SPOUSE_SALARY, SPOUSE.HOME_ADDRESS AS  SPOUSE_HOME_ADDRESS, SPOUSE.BIZ_ADDRESS AS SPOUSE_BIZ_ADDRESS,
            HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET, HOME.ADDRESS2 AS HOME_ADDRESS2, HOME.CITY AS HOME_CITY, 
            HOME.STATE AS HOME_STATE, HOME.POSTCODE AS HOME_POSTCODE, HOME.COUNTY AS HOME_COUNTY, HOME.REGION AS HOME_REGION, HOME.COUNTRY AS HOME_COUNTRY,
            BIZ.ID AS BIZ_ID, BIZ.STREET_ADDRESS AS BIZ_STREET, BIZ.ADDRESS2 AS BIZ_ADDRESS2, BIZ.CITY AS BIZ_CITY, 
            BIZ.STATE AS BIZ_STATE, BIZ.POSTCODE AS BIZ_POSTCODE, BIZ.COUNTY AS BIZ_COUNTY, BIZ.REGION AS BIZ_REGION, BIZ.COUNTRY AS BIZ_COUNTRY,
            FROM PEOPLE AS PARENT
            LEFT OUTER JOIN PEOPLE AS CHILD
            ON CHILD.PARENT_ID = PARENT.ID
            LEFT OUTER JOIN ADDRESSES AS HOME
            ON PARENT.HOME_ADDRESS = HOME.ID
            LEFT OUTER JOIN ADDRESSES AS BIZ
            ON PARENT.BIZ_ADDRESS = BIZ.ID
            LEFT OUTER JOIN PEOPLE AS SPOUSE
            ON PARENT.SPOUSE_ID = SPOUSE.ID
            WHERE PARENT.ID=?
            """;
    private static final String FIND_ALL_SQL = """
    SELECT
            PARENT.ID AS PARENT_ID, PARENT.FIRST_NAME AS PARENT_FIRST_NAME, PARENT.LAST_NAME AS PARENT_LAST_NAME,
            PARENT.DOB AS PARENT_DOB, PARENT.SALARY AS PARENT_SALARY, PARENT.EMAIL AS PARENT_EMAIL
            FROM PEOPLE AS PARENT
            LEFT OUTER JOIN PEOPLE AS CHILD
            ON CHILD.PARENT_ID = PARENT.ID
            LEFT OUTER JOIN ADDRESSES AS HOME
            ON PARENT.HOME_ADDRESS = HOME.ID
            LEFT OUTER JOIN ADDRESSES AS BIZ
            ON PARENT.BIZ_ADDRESS = BIZ.ID
            LEFT OUTER JOIN PEOPLE AS SPOUSE
            ON PARENT.SPOUSE_ID = SPOUSE.ID
            ORDER BY PARENT.ID DESC
            FETCH FIRST 100 ROWS ONLY
    """;
    private static final String COUNT_SQL = "SELECT COUNT(*) FROM PEOPLE";
    private static final String DELETE_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    private static final String DELETE_IN_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    private static final String UPDATE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=?, SPOUSE_ID=? WHERE ID=?";

    private static Map<String, Integer> aliasColIdxMap = new HashMap<>();

    public PeopleRepository(Connection connection) {
        super(connection);
        addressRepository = new AddressRepository(connection);
    }

    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    protected void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
        ps.setString(5, entity.getEmail());
        associateAddressWithPerson(entity.getHomeAddress(), ps, 6);
        associateAddressWithPerson(entity.getBusinessAddress(), ps, 7);
        associateSpouseWithPerson(entity.getSpouse(), ps, 8);
        associateChildWithParent(entity, ps, 9);
    }

    private void associateChildWithParent(Person entity, PreparedStatement ps, int i) throws SQLException {
        Optional<Person> parent = entity.getParent();
        if (parent.isPresent()) {
            ps.setLong(i, parent.get().getId());
        } else {
            ps.setObject(i, null);
        }
    }

    private void associateSpouseWithPerson(Optional<Person> spouse, PreparedStatement ps, int index) throws SQLException {
        if (spouse.isPresent()) {
            Person pSpouse = spouse.get();
            if (pSpouse.getId() == null) {
                Person savedSpouse = this.save(pSpouse);
                ps.setLong(index, savedSpouse.getId());
            }
            ps.setLong(index, pSpouse.getId());
        } else {
            ps.setObject(index, null);
        }
    }

    @Override
    protected void postSave(Person entity) {
        Optional<Person> spouse = entity.getSpouse();
        if (spouse.isPresent()) {
            spouse.get().setSpouse(entity);
            update(spouse.get());
        }
        entity.getChildren().forEach(this::save);
    }

    private void associateAddressWithPerson(Optional<Address> address, PreparedStatement ps, int index) throws SQLException {
        if (address.isPresent()) {
            Address savedAddress = addressRepository.save(address.get());
            ps.setLong(index, savedAddress.id());
        } else {
            ps.setObject(index, null);
        }
    }

    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = COUNT_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_IN_SQL, operationType = CrudOperation.DELETE_MANY)
    @SQL(value = DELETE_BY_ID_SQL, operationType = CrudOperation.DELETE_ONE)
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        Person finalParent = null;
        do {
            Person currentParent = extractPerson(rs, "PARENT");

            if (finalParent == null) {
                finalParent = currentParent;
            } else if (!finalParent.equals(currentParent)) {
                rs.previous();
                break;
            }

            Person spouse = extractPerson(rs, "SPOUSE");
            finalParent.setSpouse(spouse);

            Person child = extractPerson(rs, "CHILD");
            finalParent.addChild(child);

            Address homeAddress = extractAddress(rs, "HOME");
            Address bizAddress = extractAddress(rs, "BIZ");

            finalParent.setHomeAddress(homeAddress);
            finalParent.setBusinessAddress(bizAddress);

        } while (rs.next());
        return finalParent;
    }

    private static Person extractPerson(ResultSet rs, String personAlias) throws SQLException {
        Long personId = getValueByAlias(personAlias.concat("_ID"), rs);
        if (personId == null) return null;
        String firstName = rs.getString(personAlias.concat("_FIRST_NAME"));
        String lastName = rs.getString(personAlias.concat("_LAST_NAME"));
        ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp(personAlias.concat("_DOB")).toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal(personAlias.concat("_SALARY"));

        Person person = new Person(personId, firstName, lastName, dob, salary);
        return person;
    }

    private static Address extractAddress(ResultSet rs, String addressAlias) throws SQLException {
        Long addressId = getValueByAlias(addressAlias.concat("_ID"), rs);
        if (addressId == null) return null;
        String streetAddress = rs.getString(addressAlias.concat("_STREET"));
        String address2 = rs.getString(addressAlias.concat("_ADDRESS2"));
        String city = rs.getString(addressAlias.concat("_CITY"));
        String state = rs.getString(addressAlias.concat("_STATE"));
        String postcode = rs.getString(addressAlias.concat("_POSTCODE"));
        String county = rs.getString(addressAlias.concat("_COUNTY"));
        Region region = Region.valueOf(rs.getString(addressAlias.concat("_REGION")).toUpperCase());
        String country = rs.getString(addressAlias.concat("_COUNTRY"));
        Address address = new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
        return address;
    }

    private static <T> T getValueByAlias(String alias, ResultSet rs) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        int foundIdx = getIndexForAlias(alias, rs, columnCount);
        return foundIdx == 0 ? null : (T) rs.getObject(foundIdx);
    }

    private static int getIndexForAlias(String alias, ResultSet rs, int columnCount) throws SQLException {
        Integer foundIdx = aliasColIdxMap.getOrDefault(alias, 0);
        if (foundIdx == 0) {
            for (int i = 1; i <= columnCount; i++) {
                if (Objects.equals(rs.getMetaData().getColumnLabel(i), alias)) {
                    foundIdx = i;
                    aliasColIdxMap.put(alias, foundIdx);
                    break;
                }
            }
        }
        return foundIdx;
    }

    @Override
    @SQL(value = UPDATE_SQL, operationType = CrudOperation.UPDATE)
    protected void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());

        Optional<Person> spouse = entity.getSpouse();
        if (spouse.isPresent()) {
            ps.setLong(5, spouse.get().getId());
        } else {
            ps.setObject(5, null);
        }
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
