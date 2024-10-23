import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

// Leer fichero de personas
// Quedarme con las personas con DNI correcto y mayores de edad
// Las que no tengan DNI correcto a un fichero parquet
// Las que tengan DNI correcto pero no sean mayores, a otro fichero parquet
// Las que sean buenas...
//      Leo fichero de códigos postales
//          connection.read()
//              .option("sep", ",")
//              .option("header", "true")
//              .csv("path")
//      añado la info de este fichero a cada persona
//      guardo resultado en un parquet
//      Siempre y cuando la persona tenga un CP que exista en mi fichero de CP, sinó a otro fichero parquet.

// La idea es que tendremos 2 datasets
//      - personas con dni correcto
//      - CPs
// Y quiero hacer un JOIN de esos datasets (como si fuesen tablas de una BBDD relacional

public class ProcesarFicheroPersona {
    public static void main(String[] args) {

        // Paso 1: abrir conexión con el cluster de spark
        SparkSession connection = SparkSession.builder()
                .appName("Procesar fichero persona")
                .master("local")
                .getOrCreate();

        // Paso 2: conseguir los datos. En vez de usar RDD se usan Datasets
        Dataset<Row> data = connection.read().json("src/main/resources/personas.json");
        Dataset<Row> cps = connection.read()
                .option("sep", ",")
                .option("header", "true")
                .csv("src/main/resources/cps.csv");

        // Paso 3: tratar los datos con algo parecido a SQL

        // Paso 4: Hacer lo que se requiera con el resultado

        JavaRDD<Person> personsRDD = data.toJavaRDD()
                .map(row -> new Person(
                    row.getString(row.fieldIndex("nombre")),
                    row.getString(row.fieldIndex("apellidos")),
                    row.getLong(row.fieldIndex("edad")),
                    row.getString(row.fieldIndex("cp")),
                    row.getString(row.fieldIndex("email")),
                    row.getString(row.fieldIndex("dni")))
                );

        System.err.println("\n\n dni inválido_________________");
        JavaRDD<Person> notValidDnis = personsRDD.filter(person -> !person.isValidDni());
        Dataset<Row> notValidDnisDataset = connection.createDataFrame(notValidDnis, Person.class);
        notValidDnisDataset.show();

        JavaRDD<Person> validDnis = personsRDD.filter(Person::isValidDni);
        validDnis = validDnis.map(person -> {
            person.setDni(person.normalizeDni(true, true, "-"));
            return person;
        });

        System.err.println("\n\n dni válido y menor_________________");
        JavaRDD<Person> minors = validDnis.filter(person -> person.getEdad() < 18);
        Dataset<Row> validDniButMinor = connection.createDataFrame(minors, Person.class);
        validDniButMinor.show();


        JavaRDD<Person> peopleToProcess = validDnis.filter(person -> person.getEdad() >= 18);
        System.err.println("\n\n dni válido y mayor_________________");
        Dataset<Row> filteredAsDataset = connection.createDataFrame(peopleToProcess, Person.class);
        filteredAsDataset.show();

        // queda el Join entre peopleToProcess y CPs
        Dataset<Row> result = filteredAsDataset
                .join(cps, "cp");


        Dataset<Row> cpNotDetected = filteredAsDataset.select("nombre", "apellidos", "edad", "dni", "cp", "email")
                .except(result.select("nombre", "apellidos", "edad", "dni", "cp", "email"));

        result.show();
        cpNotDetected.show();

        // para evitar que se cierre el master, puedo poner un thread sleep y que me deje ver la UI en local
        try{
            Thread.sleep(1000*60*60);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // Paso 5: cerrar conexión
        connection.close();
    }
}
