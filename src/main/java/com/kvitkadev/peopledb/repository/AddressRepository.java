package com.kvitkadev.peopledb.repository;

import com.kvitkadev.peopledb.annotation.SQL;
import com.kvitkadev.peopledb.model.Address;
import com.kvitkadev.peopledb.model.CrudOperation;
import com.kvitkadev.peopledb.model.Region;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AddressRepository extends CrudRepository<Address> {
    public AddressRepository(Connection connection) {
        super(connection);
    }

    @Override
    @SQL(operationType = CrudOperation.FIND_BY_ID, value = """
            SELECT ID, STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY 
            FROM ADDRESSES 
            WHERE ID=?
            """)
    Address extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long id = rs.getLong("ID");
        String streetAddress = rs.getString("STREET_ADDRESS");
        String address2 = rs.getString("ADDRESS2");
        String city = rs.getString("CITY");
        String state = rs.getString("STATE");
        String postcode = rs.getString("POSTCODE");
        String county = rs.getString("COUNTY");
        Region region = Region.valueOf(rs.getString("REGION").toUpperCase());
        String country = rs.getString("COUNTRY");
        return new Address(id, streetAddress, address2, city, state, postcode, country, county, region);
    }

    @Override
    @SQL(operationType = CrudOperation.SAVE, value = """
        INSERT INTO ADDRESSES (STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY) VALUES (?,?,?,?,?,?,?,?)
        """)
    protected void mapForSave(Address address, PreparedStatement ps) throws SQLException {
        ps.setString(1, address.streetAddress());
        ps.setString(2, address.address2());
        ps.setString(3, address.city());
        ps.setString(4, address.state());
        ps.setString(5, address.postcode());
        ps.setString(6, address.county());
        ps.setString(7, address.region().toString());
        ps.setString(8, address.country());
    }

    @Override
    protected void mapForUpdate(Address entity, PreparedStatement ps) throws SQLException {

    }
}
