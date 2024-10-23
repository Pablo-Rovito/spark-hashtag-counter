import lombok.*;
import org.apache.hadoop.shaded.org.checkerframework.checker.nullness.Opt;

import javax.swing.text.html.Option;
import java.io.Serializable;
import java.util.Optional;

@Getter
@EqualsAndHashCode
@ToString
@NoArgsConstructor
public class Person implements Serializable {
    @Setter private String nombre;
    @Setter private String apellidos;
    @Setter private Long edad;
    @Setter private String cp;
    @Setter private String email;
    private String dni;
    private int dniNumber;
    private String dniChar;
    private boolean validDni = false;

    public Person(String nombre, String apellidos, long edad, String cp, String email, String dni) {
        setNombre(nombre);
        setApellidos(apellidos);
        setEdad(edad);
        setCp(cp);
        setEmail(email);
        setDni(dni);
    }

    public void setDni(String dni) {
        this.dni = dni;
        validateDni(dni);
    }

    private void validateDni(String dni) {
        dni = dni.toUpperCase();
        this.validDni = dni.matches("^(([0-9]{1,8})|((([0-9]{1,2}[.][0-9]{3})|([0-9]{1,3}))[.][0-9]{3}))[ -]?[A-Z]$");
        if(validDni) {
            dni = dni.replaceAll("[. -]", "");
            this.dniNumber = Integer.parseInt(dni.substring(0, dni.length() -1));
            this.dniChar = dni.substring(dni.length() -1);
            this.validDni = LETRAS_DNI.charAt(dniNumber%23) == dniChar.charAt(0);
        }
    }

    public static boolean validate(String dni) {
        dni = dni.toUpperCase();
        boolean valid = dni.matches("^(([0-9]{1,8})|((([0-9]{1,2}[.][0-9]{3})|([0-9]{1,3}))[.][0-9]{3}))[ -]?[A-Z]$");
        if(!valid) {
            return false;
        } else {
            dni = dni.replaceAll("[. -]", "");
            int dniNumber = Integer.parseInt(dni.substring(0, dni.length() -1));
            String dniChar = dni.substring(dni.length() -1);
            valid = LETRAS_DNI.charAt(dniNumber%23) == dniChar.charAt(0);
        }
        return valid;
    }

    public String normalizeDni(boolean thousandDot, boolean leftZeroes, String charSeparator) {
        if(!validDni) {
            return null;
        }
        String result = (charSeparator == null ? "" : charSeparator) + dniChar;
        String normalizedDniNumber = ""+dniNumber;

        if(leftZeroes) {
            normalizedDniNumber = ("00000000" + normalizedDniNumber).substring(normalizedDniNumber.length());
        }
        if(thousandDot) {
            int position = normalizedDniNumber.length() - 3;
            while(position > 0) {
                normalizedDniNumber = normalizedDniNumber.substring(0, position) + "." + normalizedDniNumber.substring(position);
                position -=3;
            }
        }
        return normalizedDniNumber + result;
    }

    private static final String LETRAS_DNI = "TRWAGMYFPDXBNJZSQVHLCKE";
}
