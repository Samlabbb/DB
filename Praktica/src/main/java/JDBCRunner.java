import java.sql.*;

public class JDBCRunner {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "StarWars";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;

    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            getStormtroopers(connection);
            System.out.println();
            getMasters(connection);
            System.out.println();
            getCamps(connection);
            System.out.println();
            getStormtroopersNamed(connection, "Djoch", false);
            System.out.println();
            getStormtrooperNamed(connection, "Djoch", false);
            System.out.println();
            addMaster(connection, "Master of waterfulllll", "sword", 1, 5);
            System.out.println();
            deleteMasters(connection, 5);
            System.out.println();
            deleteStormtrooper(connection, "Joja");
            System.out.println();
            addStormtrooper(connection, "Joja", "Dubin", "gun", 1, "Master of water");
            System.out.println();
            correctStormtrooper(connection, "PAYPAY", "Yarick");
            System.out.println();
            correctMaster(connection, "Bulava", "Lord of Fire");
            System.out.println();
            correctCamp(connection, 20, 1);
            System.out.println();
            deleteCamp(connection, 1);
            System.out.println();
            addCamp(connection, "Lord of Fire", 1);
            System.out.println();

        } catch (SQLException e) {//Здесь отлавливаем дублирование данных
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }

    }

    public static void checkDriver() {//Проверка на подключение JDBC
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    private static void getStormtroopers(Connection connection) throws SQLException {//Метод для взятия всех штурмовиков
        // имена столбцов
        String columnName0 = "sequence_number", columnName1 = "name", columnName2 = "surname", columnName3 = "weapon", columnName4 = "camp", columnName5 = "master";
        // значения ячеек
        int param0 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null, param5 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM stormtroopers;");

        while (rs.next()) {
            param2 = rs.getString(columnName2);
            param1 = rs.getString(columnName1);
            param3 = rs.getString(columnName3);
            param4 = rs.getString(columnName4);
            param5 = rs.getString(columnName5);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }

    private static void getMasters(Connection connection) throws SQLException {//Метод для взятия всех мастеров
        // имена столбцов
        String columnName0 = "name_of_master", columnName1 = "camp_id", columnName2 = "master_weapon", columnName3 = "number_of_subordinates";
        // значения ячеек
        int param0 = -1, param2 = -1;
        String param1 = null, param3 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM masters;");

        while (rs.next()) {
            param3 = rs.getString(columnName2);
            param0 = rs.getInt(columnName1);
            param2 = rs.getInt(columnName3);
            param1 = rs.getString(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }

    private static void getCamps(Connection connection) throws SQLException {//Метод для взятия всех лагерей
        // имена столбцов
        String columnName0 = "id", columnName1 = "Number_of_fighters", columnName2 = "Masters";
        // значения ячеек
        int param0 = -1, param2 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM camps;");

        while (rs.next()) {
            param0 = rs.getInt(columnName0);
            param2 = rs.getInt(columnName1);
            param1 = rs.getString(columnName2);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

    private static void getStormtroopersNamed(Connection connection, String name, boolean fromSQL) throws SQLException {//Метод для взятия всех штурмовиков по имени с фамилией
        if (name == null || name.isBlank()) return;

        if (fromSQL) {
            getStormtroopersNamed(connection, name, fromSQL);
        } else {
            long time = System.currentTimeMillis();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(
                    "SELECT sequence_number, name, surname, weapon, camp, master FROM stormtroopers");
            while (rs.next()) {
                if (rs.getString(2).contains(name)) {
                    System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
                }
            }
            System.out.println("SELECT ALL and FIND (" + (System.currentTimeMillis() - time) + " мс.)");
        }
    }

    private static void getStormtrooperNamed(Connection connection, String name, boolean b) throws SQLException {//Метод для взятия всех штурмовиков без фамилии
        if (name == null || name.isBlank()) return;
        name = '%' + name + '%';

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT sequence_number, name, surname, weapon, camp, master FROM stormtroopers WHERE name LIKE ?;");
        statement.setString(1, name);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getInt(1) + " | " + rs.getString(2));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }


    private static void deleteMasters(Connection connection, Integer coun) throws SQLException {//Метод для удаления мастера по количеству штурмовиков
        if (coun <= 0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE FROM masters WHERE number_of_subordinates = ?");
        statement.setInt(1, coun);

        int count = statement.executeUpdate();
        System.out.println("DELITEd" + count + " masters");
        getMasters(connection);
    }

    private static void addStormtrooper(Connection connection, String name, String surname, String weapon, Integer camp, String master) throws SQLException {//Метод для добавления штурмовика
        if (name == null || name.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("INSERT INTO stormtroopers(name, surname, weapon, camp, master) VALUES (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setString(2, surname);
        statement.setString(3, weapon);
        statement.setInt(4, camp);
        statement.setString(5, master);

        int count = statement.executeUpdate();
        long time = System.currentTimeMillis();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("INSERTed " + count + " stormtroopers" + (System.currentTimeMillis() - time) + "ms");
        }
        getStormtroopers(connection);

    }

    private static void deleteStormtrooper(Connection connection, String name) throws SQLException {//Метод для удаления штурмовика по имени
        if (name == null) return;

        PreparedStatement statement = connection.prepareStatement("DELETE FROM stormtroopers WHERE name = ?");
        statement.setString(1, name);

        int count = statement.executeUpdate();
        System.out.println("DELITEd " + count + " stormtroopers");
        getStormtroopers(connection);
    }

    private static void addMaster(Connection connection, String name, String weapon, Integer camp_id, Integer values) throws SQLException {//Метод для добавления мастера
        if (name == null || name.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("INSERT INTO masters(name_of_master,camp_id, master_weapon, number_of_subordinates) VALUES (?, ?, ?, ?) returning name_of_master", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setInt(2, camp_id);
        statement.setString(3, weapon);
        statement.setInt(4, values);


        int count = statement.executeUpdate();
        long time = System.currentTimeMillis();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("INSERTed " + count + " masters" + (System.currentTimeMillis() - time) + "ms");
        }
        getMasters(connection);
    }

    private static void correctStormtrooper(Connection connection, String weapon, String name) throws SQLException {//Метод для корректировки штурмовика по оружию
        if (weapon == null || weapon.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE stormtroopers SET weapon=? WHERE name=?;");
        statement.setString(1, weapon); // сначала что передаем
        statement.setString(2, name); // затем по чему ищем


        int count = statement.executeUpdate();

        System.out.println("UPDATEd " + count + " stormtroopers");
        getStormtroopers(connection);
    }

    private static void correctMaster(Connection connection, String weapon, String name) throws SQLException {//Метод для корректировки мастера по оружию
        if (weapon == null || weapon.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE masters SET master_weapon=? WHERE name_of_master=?;");
        statement.setString(1, weapon);
        statement.setString(2, name);


        int count = statement.executeUpdate();

        System.out.println("UPDATEd " + count + " masters");
        getMasters(connection);
    }

    private static void correctCamp(Connection connection, Integer values, Integer id) throws SQLException {//Метод для корректировки лагеря по кол-ву штурмовиков
        if (values <= 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE camps SET number_of_fighters=? WHERE id=?;");
        statement.setInt(1, values);
        statement.setInt(2, id);


        int count = statement.executeUpdate();

        System.out.println("UPDATEd " + count + " camps");
        getCamps(connection);
    }
    private static void deleteCamp(Connection connection, Integer coun) throws SQLException {//Метод для удаления лагеря
        if (coun <= 0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE FROM camps WHERE number_of_fighters = ?");
        statement.setInt(1, coun);

        int count = statement.executeUpdate();
        System.out.println("DELITEd " + count + " camps");
        getCamps(connection);
    }
    private static void addCamp(Connection connection, String name , Integer id) throws SQLException {//Метод для добавления лагеря
        if (name == null || name.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("INSERT INTO camps(number_of_fighters, masters) VALUES (?, ?) returning number_of_fighters", Statement.RETURN_GENERATED_KEYS);
        statement.setString(2, name);
        statement.setInt(1, id);

        int count = statement.executeUpdate();
        long time = System.currentTimeMillis();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("INSERTed " + count + " camps " + (System.currentTimeMillis() - time) + " ms");
        }
        getCamps(connection);
    }
}

