package de.hpi.ddm.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Line implements Serializable {
    private static final long serialVersionUID = -50168550560230172L;
    int id;
    String name;
    List<String> passwordChars;
    int passwordLength;
    String hashedPassword;
    String cleartextPassword;
    List<String> hints;
    LinkedList<Character> charsNotInPasswordFromHints = new LinkedList<>();
}
