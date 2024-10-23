import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

public class IntroSparkSql {
    public static void main(String[] args) {

        // Paso 1: abrir conexión con el cluster de spark
        SparkSession connection = SparkSession.builder()
                .appName("Introducción a SparkSQL")
                .master("local[2]")
                .getOrCreate();

        // Paso 2: conseguir los datos. En vez de usar RDD se usan Datasets
        Dataset<Row> data = connection.read().json("src/main/resources/personas.json");

        // Paso 3: tratar los datos con algo parecido a SQL

        Dataset<Row> result = data;

        // Paso 4: Hacer lo que se requiera con el resultado

        // se puede armar con SQL en vez de con select()
        data.createOrReplaceTempView("personas");
        connection.sql("SELECT nombre,edad FROM personas WHERE edad > 30").show();
//        connection.sql("SELECT nombre,edad FROM personas WHERE edad > 30")
//                .write()
//                .json("src/main/resources/result.json");

        JavaRDD<Person> personsRDD = data.toJavaRDD()
                .map(row -> new Person(
                    row.getString(row.fieldIndex("nombre")),
                    row.getString(row.fieldIndex("apellidos")),
                    row.getLong(row.fieldIndex("edad")),
                    row.getString(row.fieldIndex("cp")),
                    row.getString(row.fieldIndex("email")),
                    row.getString(row.fieldIndex("dni")))
                );
        JavaRDD<Person> validDnis = personsRDD.filter(Person::isValidDni);
        validDnis = validDnis.map(person -> {
            person.setDni(person.normalizeDni(true, true, "-"));
            return person;
        });

        // Aplicar el filtro sobre un Dataset ahora...
        // SparkSQL tiene el concepto de User Defined Functions UDF
        // UDF es una función que yo defino para usar en mis consultas SQL
        // Para ello tengo que registrarla en mi contexto de SparkSQL
        // Esto no está andando
        var miFuncionValidacion = udf((String dni) -> Person.validate(dni), DataTypes.BooleanType);
        connection.udf().register("isValidDni", miFuncionValidacion);
        connection.sql("SELECT nombre,dni FROM personas WHERE isValidDni(dni)").show();

        JavaRDD<Person> filteredPersonsRDD = personsRDD.filter(person -> person.getEdad() > 30);
        Dataset<Row> filteredAsDataset = connection.createDataFrame(validDnis, Person.class);
        filteredAsDataset.show();

        result.select(col("apellidos"), col("edad")).filter(col("edad").gt(30)).show();
        result.groupBy(col("nombre")).sum("edad").orderBy(col("sum(edad)").desc()).show();

        // Paso 5: cerrar conexión
        connection.close();
    }
}
