package com.kvitkadev.peopledb.repository;

import com.kvitkadev.peopledb.annotation.Id;
import com.kvitkadev.peopledb.annotation.MultiSQL;
import com.kvitkadev.peopledb.annotation.SQL;
import com.kvitkadev.peopledb.exception.DataException;
import com.kvitkadev.peopledb.exception.UnableToSaveException;
import com.kvitkadev.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

abstract class CrudRepository<T> {
    protected Connection connection;
    protected PreparedStatement findPs;

    public CrudRepository(Connection connection) {
        this.connection = connection;
        try {
            this.findPs = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSql));
        } catch (SQLException e) {
            throw new DataException("Unable to create prepared statements for CrudRepository", e);
        }
    }

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.SAVE, this::getSaveSql),
                    PreparedStatement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()) {
                long id = rs.getLong(1);
                setIdByAnnotation(entity, id);
                postSave(entity);
//                System.out.println(entity);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: " + entity);
        }

        return entity;
    }

    public Optional<T> findById(Long id) {
        T foundEntity = null;
        try {
            findPs.setLong(1, id);
            ResultSet rs = findPs.executeQuery();
            while (rs.next()) {
                foundEntity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(foundEntity);
    }

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();
        try {
            PreparedStatement ps = connection.prepareStatement(
                    getSqlByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSql),
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                T entity = extractEntityFromResultSet(rs);
                entities.add(entity);

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return entities;
    }

    public long count() {
        long count = 0L;
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.COUNT, this::getCountSql));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE_ONE, this::getDeleteById));
            ps.setLong(1, getIdByAnnotation(entity));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Long getIdByAnnotation(T entity) {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .map (f -> {
                    f.setAccessible(true);
                    Long id = null;
                    try {
                        id = (long) f.get(entity);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No ID annotated id found"));
    }

    private void setIdByAnnotation(T entity, Long id) {
        Arrays.stream(entity.getClass().getDeclaredFields())
            .filter(f -> f.isAnnotationPresent(Id.class))
            .forEach (f -> {
                f.setAccessible(true);
                try {
                    f.set(entity, id);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Unable to set id value");
                }
            });
    }

    public void delete(T... entities) {
        try {
            Statement stmt = connection.createStatement();
            String ids = Arrays.stream(entities)
                    .map(this::getIdByAnnotation)
                    .map(String::valueOf)
                    .collect(joining(", "));
            stmt.executeUpdate(getSqlByAnnotation(CrudOperation.DELETE_MANY, this::getDeleteInSql).replace(":ids", ids));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.UPDATE, this::getUpdateSql));
            mapForUpdate(entity, ps);
            ps.setLong(6, getIdByAnnotation(entity));
            int affectedRecordCount = ps.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSqlByAnnotation(CrudOperation operationTYpe, Supplier<String> sqlGetter) {
        Stream<SQL> multiSqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(MultiSQL.class))
                .map(m -> m.getAnnotation(MultiSQL.class))
                .map(msql -> msql.value())
                .flatMap(Arrays::stream);


        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(SQL.class))
                .map(m -> m.getAnnotation(SQL.class));

        return Stream.concat(multiSqlStream, sqlStream)
                .filter(a -> a.operationType().equals(operationTYpe))
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter);
    }

    /**
     *
     * @return Should return a SQL string like
     * "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * be sure to include '(:ids)' named parameter & call it 'ids'
     */
    protected String getDeleteInSql(){throw new RuntimeException("SQL not defined;");}

    protected String getDeleteById(){throw new RuntimeException("SQL not defined;");};

    protected String getFindAllSql(){throw new RuntimeException("SQL not defined;");};

    protected String getCountSql(){throw new RuntimeException("SQL not defined;");};

    /**
     *
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL perameter, i.e. "?", that will bind Entity id
     */
    protected String getFindByIdSql(){throw new RuntimeException("SQL not defined;");};

    protected String getSaveSql() { throw new RuntimeException("SQL not defined;");}

    protected String getUpdateSql() { throw new RuntimeException("SQL not defined;");}

    protected void postSave(T entity) {
    }
    abstract T extractEntityFromResultSet (ResultSet rs) throws SQLException;

    protected abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    protected abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;
}
