package herddb.sql.functions;

import herddb.core.AbstractTableManager;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import herddb.model.TableDoesNotExistException;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;


/**
 * Utility class that is responsible for generating Show Create Table command
 * @author amitchavan
 */
public class ShowCreateTableCalculator {

    public static String calculate(boolean showCreateIndex, String tableName, String tableSpace, AbstractTableManager tableManager) {

        Table t  = tableManager.getTable();
        if (t == null) {
            throw new TableDoesNotExistException(String.format("Table %s does not exist.", tableName));
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE "+tableSpace+"."+tableName);
        StringJoiner joiner = new StringJoiner(",","(", ")");
        for(Column c : t.getColumns()) {
            joiner.add(c.name + " "+ ColumnTypes.typeToString(c.type)  +autoIncrementColumn(t,c));
        }

        if (t.getPrimaryKey().length > 0) {
            joiner.add("PRIMARY KEY("+ Arrays.stream(t.getPrimaryKey()).collect(Collectors.joining(",")) + ")");
        }

        if (showCreateIndex) {
            List<Index> indexes = tableManager.getAvailableIndexes();

            if (!indexes.isEmpty()) {
                indexes.forEach( idx -> {
                    joiner.add("INDEX "+idx.name + "("+Arrays.stream(idx.columnNames).collect(Collectors.joining(",")) + ")");
                });
            }
        }

        sb.append(joiner.toString());
        return sb.toString();
    }

    private static String autoIncrementColumn(Table t, Column c) {
        if (t.auto_increment && c.name.equals(t.primaryKey[0]) && (c.type == ColumnTypes.INTEGER || c.type == ColumnTypes.NOTNULL_INTEGER ||
                c.type == ColumnTypes.LONG || c.type == ColumnTypes.NOTNULL_LONG )) {
            return " auto_increment";
        }
        return "";
    }
}
